/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Optimized open-addressing (linear-probing) hash-table dictionary for string columns.
 * Strings are stored as UTF-8 bytes in a single append-only byte array.
 *
 * <p>Performance improvements over {@link StringHashTableDictionary} (chaining):
 * <ul>
 *   <li><b>Flat {@code int[]} hash table</b> – eliminates the array of
 *       {@link DynamicIntArray} bucket objects and their per-element
 *       chunk-division overhead.</li>
 *   <li><b>Power-of-two capacity</b> – {@code hash &amp; mask} replaces
 *       {@code Math.floorMod(hash, capacity)}, turning a software division
 *       into a single bitwise AND.</li>
 *   <li><b>FNV-1a hash function</b> – better bit avalanche than the previous
 *       polynomial, reducing average probe-chain length.</li>
 *   <li><b>Per-slot hash fingerprint</b> ({@code slotHashes}) – lets the
 *       hot loop reject non-matching slots without touching key bytes.</li>
 *   <li><b>Explicit key-length storage</b> ({@code keyLengths}) – avoids
 *       computing length from two adjacent offset lookups on every probe.</li>
 *   <li><b>O(capacity) {@code clear()}</b> – {@link Arrays#fill} on the flat
 *       array instead of re-allocating thousands of bucket objects.</li>
 *   <li><b>O(size) {@code resize()}</b> – allocates only two new {@code int[]}
 *       arrays instead of {@code newCapacity} {@link DynamicIntArray} objects.</li>
 * </ul>
 *
 * <p>This implementation is not thread-safe.
 * @since 1.9.0
 * @see StringHashTableDictionary
 */
public class StringHashTableDictionaryV2 implements Dictionary {

  /** Sentinel value meaning "empty slot". Key indices are stored as {@code keyIndex + 1}. */
  private static final int EMPTY = 0;

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /**
   * The maximum table size to allocate, matching {@link java.util.Hashtable}.
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  // --- Key storage (never reorganised; only appended) ---

  /** Raw UTF-8 bytes of all keys, stored contiguously. */
  private final DynamicByteArray byteArray = new DynamicByteArray();

  /** Byte offset of each key inside {@link #byteArray}, indexed by keyIndex. */
  private final DynamicIntArray keyOffsets;

  /**
   * Byte length of each key, indexed by keyIndex.
   * Stored explicitly so that equality checks need only one lookup instead
   * of two adjacent offset reads, and length can short-circuit before
   * a full byte comparison.
   */
  private final DynamicIntArray keyLengths;

  // --- Open-addressing hash table ---

  /**
   * Flat hash table. {@code hashTable[slot] == EMPTY} means the slot is
   * unoccupied; otherwise it holds {@code keyIndex + 1}.
   */
  private int[] hashTable;

  /**
   * FNV-1a fingerprint of the key stored in each slot. A fingerprint mismatch
   * allows most non-matching probes to skip the full byte comparison.
   */
  private int[] slotHashes;

  // --- Table metadata ---

  /** Always a power of two. */
  private int capacity;

  /** {@code capacity - 1}; used for fast {@code hash & mask} modulo. */
  private int mask;

  /** Number of distinct keys currently stored. */
  private int size;

  private final float loadFactor;

  /** Trigger a resize when {@link #size} reaches this value. */
  private int threshold;

  // -------------------------------------------------------------------------

  public StringHashTableDictionaryV2(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public StringHashTableDictionaryV2(int initialCapacity, float loadFactor) {
    this.loadFactor = loadFactor;
    this.capacity = tableSizeFor(initialCapacity);
    this.mask = this.capacity - 1;
    this.keyOffsets = new DynamicIntArray(initialCapacity);
    this.keyLengths = new DynamicIntArray(initialCapacity);
    this.hashTable = new int[this.capacity];
    this.slotHashes = new int[this.capacity];
    this.threshold = (int) Math.min((double) this.capacity * loadFactor, MAX_ARRAY_SIZE + 1L);
  }

  // -------------------------------------------------------------------------
  // Dictionary interface
  // -------------------------------------------------------------------------

  @Override
  public void visit(Visitor visitor) throws IOException {
    final VisitorContextImpl context =
        new VisitorContextImpl(this.byteArray, this.keyOffsets);
    for (int slot = 0; slot < capacity; slot++) {
      if (hashTable[slot] != EMPTY) {
        context.setPosition(hashTable[slot] - 1);
        visitor.visit(context);
      }
    }
  }

  @Override
  public void clear() {
    byteArray.clear();
    keyOffsets.clear();
    keyLengths.clear();
    // Zero out the occupied-slot markers; slotHashes values are irrelevant
    // for empty slots and do not need to be cleared.
    Arrays.fill(hashTable, EMPTY);
    size = 0;
  }

  @Override
  public void getText(Text result, int positionInKeyOffset) {
    DictionaryUtils.getTextInternal(result, positionInKeyOffset,
        this.keyOffsets, this.byteArray);
  }

  @Override
  public ByteBuffer getText(int positionInKeyOffset) {
    return DictionaryUtils.getTextInternal(positionInKeyOffset,
        this.keyOffsets, this.byteArray);
  }

  @Override
  public int writeTo(OutputStream out, int position) throws IOException {
    return DictionaryUtils.writeToTextInternal(out, position,
        this.keyOffsets, this.byteArray);
  }

  public int add(Text text) {
    return add(text.getBytes(), 0, text.getLength());
  }

  @Override
  public int add(final byte[] bytes, final int offset, final int length) {
    if (size >= threshold) {
      resize();
    }

    final int hash = fnvHash(bytes, offset, length);
    int slot = getIndex(bytes, offset, length, hash);

    while (hashTable[slot] != EMPTY) {
      // Fast rejection: compare fingerprints before touching key bytes.
      if (slotHashes[slot] == hash) {
        final int keyIdx = hashTable[slot] - 1;
        final int keyLen = keyLengths.get(keyIdx);
        if (keyLen == length) {
          final int keyOff = keyOffsets.get(keyIdx);
          if (byteArray.compare(bytes, offset, length, keyOff, keyLen) == 0) {
            return keyIdx;
          }
        }
      }
      slot = (slot + 1) & mask;
    }

    // Not found – insert at this empty slot.
    final int keyIdx = size;
    keyOffsets.add(byteArray.add(bytes, offset, length));
    keyLengths.add(length);
    hashTable[slot] = keyIdx + 1;
    slotHashes[slot] = hash;
    size++;
    return keyIdx;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public long getSizeInBytes() {
    return byteArray.getSizeInBytes()
        + keyOffsets.getSizeInBytes()
        + keyLengths.getSizeInBytes()
        + (long) hashTable.length * Integer.BYTES
        + (long) slotHashes.length * Integer.BYTES;
  }

  // -------------------------------------------------------------------------
  // Hash computation and initial slot mapping
  // -------------------------------------------------------------------------

  /**
   * FNV-1a (32-bit) hash. Compared with a Horner polynomial (multiplier 31),
   * FNV-1a has stronger bit avalanche, reducing the average probe-chain length.
   * The result is guaranteed non-zero so it can serve directly as the
   * {@link #slotHashes} fingerprint.
   */
  private static int fnvHash(byte[] bytes, int offset, int length) {
    int h = 0x811c9dc5;
    for (int i = offset, end = offset + length; i < end; i++) {
      h ^= bytes[i] & 0xFF;
      h *= 0x01000193;
    }
    // Ensure the result is never EMPTY (0) to keep the sentinel unambiguous.
    return (h == 0) ? 1 : h;
  }

  /**
   * Returns the initial slot index for the given key and its pre-computed hash.
   * The default implementation uses a power-of-two modulo ({@code hash & mask}),
   * which the JIT can emit as a single AND instruction.
   *
   * <p>Package-private so that test subclasses can override the slot-selection
   * strategy to make collision behaviour deterministic. Subclasses that need
   * the raw key bytes (e.g. to derive a deterministic slot from the key content)
   * should override this method; the {@code hash} parameter may be ignored.
   *
   * <p><strong>Important limitation for subclasses:</strong> this method is
   * called only from {@link #add}. The {@link #resize} path bypasses it and
   * always places rehashed entries using {@code storedHash & mask} directly,
   * because the key bytes are not re-read during rehashing. Consequently, if a
   * subclass overrides this method to derive the initial slot from the key
   * content rather than from the hash, the slot placement after a resize will
   * diverge from what {@link #add} would choose, potentially causing duplicate
   * insertions after the resize. Test subclasses should therefore be designed
   * to keep the number of insertions below the resize threshold.
   */
  int getIndex(final byte[] bytes, final int offset, final int length, final int hash) {
    return hash & mask;
  }

  /**
   * Convenience overload: computes the FNV-1a hash internally and delegates.
   * Callers inside {@link #add} should prefer the four-argument overload to
   * avoid computing the hash twice.
   */
  int getIndex(final byte[] bytes, final int offset, final int length) {
    return getIndex(bytes, offset, length, fnvHash(bytes, offset, length));
  }

  /** Convenience overload for callers that already hold a {@link Text}. */
  int getIndex(Text text) {
    return getIndex(text.getBytes(), 0, text.getLength());
  }

  // -------------------------------------------------------------------------
  // Internal helpers
  // -------------------------------------------------------------------------

  /** Returns the smallest power of two &ge; {@code n} (minimum 1). */
  private static int tableSizeFor(int n) {
    if (n <= 1) return 1;
    int p = Integer.highestOneBit(n - 1) << 1;
    return (p > 0) ? p : MAX_ARRAY_SIZE;
  }

  /**
   * Doubles the table capacity and rehashes all existing entries.
   *
   * <p>Note: {@link #getIndex} is <em>not</em> called during rehashing.
   * Each occupied slot is repositioned using its stored FNV-1a fingerprint
   * ({@code slotHashes[i] & newMask}) directly, without re-reading key bytes.
   * This avoids the cost of re-reading key bytes and recomputing hashes, but
   * means that any {@link #getIndex} override in a test subclass will not
   * affect slot placement after a resize.
   */
  private void resize() {
    if (capacity >= MAX_ARRAY_SIZE) {
      // Cannot grow further; prevent repeated resize attempts.
      threshold = Integer.MAX_VALUE;
      return;
    }

    final int oldCapacity = this.capacity;
    final int[] oldHashTable = this.hashTable;
    final int[] oldSlotHashes = this.slotHashes;

    final int newCapacity = oldCapacity << 1;
    final int[] newHashTable = new int[newCapacity];
    final int[] newSlotHashes = new int[newCapacity];

    // Update capacity and mask before the rehash loop so that this.mask
    // reflects the new (doubled) capacity when computing newSlot below.
    this.capacity = newCapacity;
    this.mask = newCapacity - 1;
    this.hashTable = newHashTable;
    this.slotHashes = newSlotHashes;
    this.threshold = (int) Math.min((double) newCapacity * loadFactor, MAX_ARRAY_SIZE + 1L);

    for (int i = 0; i < oldCapacity; i++) {
      if (oldHashTable[i] == EMPTY) {
        continue;
      }
      // The FNV-1a hash is already stored in oldSlotHashes[i]; reuse it
      // directly instead of re-reading key bytes and recomputing the hash.
      int newSlot = oldSlotHashes[i] & this.mask;
      while (newHashTable[newSlot] != EMPTY) {
        newSlot = (newSlot + 1) & this.mask;
      }
      newHashTable[newSlot] = oldHashTable[i];
      newSlotHashes[newSlot] = oldSlotHashes[i];
    }
  }
}
