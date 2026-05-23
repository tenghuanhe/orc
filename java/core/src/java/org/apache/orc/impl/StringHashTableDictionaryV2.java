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
 * Open-addressing (linear-probing) hash-table dictionary for string columns.
 * Strings are stored contiguously as UTF-8 bytes in an append-only byte array.
 *
 * <p>Improvements over {@link StringHashTableDictionary} (chaining):
 * <ul>
 *   <li><b>Flat {@code int[]} table</b> – no per-bucket {@link DynamicIntArray} objects.</li>
 *   <li><b>Power-of-two capacity</b> – {@code hash &amp; mask} replaces {@code floorMod}.</li>
 *   <li><b>FNV-1a hash</b> – stronger avalanche reduces average probe-chain length.</li>
 *   <li><b>Per-slot fingerprint</b> ({@code slotHashes}) – rejects mismatches before byte comparison.</li>
 *   <li><b>Explicit key lengths</b> ({@code keyLengths}) – enables early-exit on length mismatch.</li>
 *   <li><b>O(capacity) {@code clear()}</b> – {@link Arrays#fill} on the flat array.</li>
 *   <li><b>O(size) {@code resize()}</b> – allocates two new arrays, not {@code newCapacity} objects.</li>
 * </ul>
 *
 * <p>Not thread-safe.
 * @since 1.9.0
 * @see StringHashTableDictionary
 */
public class StringHashTableDictionaryV2 implements Dictionary {

  /** Empty-slot sentinel; occupied slots store {@code keyIndex + 1}. */
  private static final int EMPTY = 0;

  private static final float DEFAULT_LOAD_FACTOR = 0.75f;

  /** Max capacity (power of two); doubling beyond this overflows a signed 32-bit int. */
  private static final int MAXIMUM_CAPACITY = 1 << 30;


  // --- Key storage (append-only) ---

  /** All key bytes stored contiguously. */
  private final DynamicByteArray byteArray = new DynamicByteArray();

  /** Start offset of each key in {@link #byteArray}, indexed by keyIndex. */
  private final DynamicIntArray keyOffsets;

  /**
   * Byte length of each key, indexed by keyIndex.
   * Stored explicitly to avoid computing length from adjacent offsets and to
   * enable early-exit on length mismatch during probing.
   */
  private final DynamicIntArray keyLengths;

  // --- Open-addressing hash table ---

  /** {@code hashTable[slot] == EMPTY} → unoccupied; otherwise {@code keyIndex + 1}. */
  private int[] hashTable;

  /** FNV-1a fingerprint per slot; allows the probe loop to skip byte comparison on mismatch. */
  private int[] slotHashes;

  // --- Table metadata ---

  /** Always a power of two. */
  private int capacity;

  /** {@code capacity - 1}; used for fast {@code hash & mask} modulo. */
  private int mask;

  /** Number of distinct keys stored. */
  private int size;

  private final float loadFactor;

  /** Resize when {@link #size} reaches this value. */
  private int threshold;

  // -------------------------------------------------------------------------

  public StringHashTableDictionaryV2(int initialCapacity) {
    this(initialCapacity, DEFAULT_LOAD_FACTOR);
  }

  public StringHashTableDictionaryV2(int initialCapacity, float loadFactor) {
    this.loadFactor = loadFactor;
    this.capacity = tableSizeFor(initialCapacity);
    this.mask = this.capacity - 1;
    this.keyOffsets = new DynamicIntArray(Math.max(1, initialCapacity));
    this.keyLengths = new DynamicIntArray(Math.max(1, initialCapacity));
    this.hashTable = new int[this.capacity];
    this.slotHashes = new int[this.capacity];
    this.threshold = maxFill(this.capacity, loadFactor);
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
    // slotHashes need not be cleared; values in empty slots are never read
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
   * FNV-1a 32-bit hash. Result is always non-zero (0 replaced by 1) so it
   * doubles as the {@link #slotHashes} fingerprint without ambiguity.
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
   * Returns {@code hash & mask} as the initial probe slot.
   *
   * <p>Package-private for test subclasses that need deterministic slot placement.
   * <strong>Important:</strong> {@link #resize} bypasses this method and uses
   * stored fingerprints directly, so subclass overrides do not affect placement
   * after a resize. Test subclasses must keep size below the resize threshold.
   */
  int getIndex(final byte[] bytes, final int offset, final int length, final int hash) {
    return hash & mask;
  }

  /** Computes the FNV-1a hash then delegates to {@link #getIndex(byte[], int, int, int)}. */
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

  /** Returns the smallest power of two &ge; {@code n} (minimum 1, maximum {@link #MAXIMUM_CAPACITY}). */
  private static int tableSizeFor(int n) {
    if (n <= 1) return 1;
    int p = Integer.highestOneBit(n - 1) << 1;
    return (p > 0 && p <= MAXIMUM_CAPACITY) ? p : MAXIMUM_CAPACITY;
  }

  /**
   * Max entries before resize: {@code min(ceil(n * f), n - 1)}, mirroring fastutil's
   * {@code HashCommon#maxFill}.
   *
   * <p>The {@code n - 1} cap ensures at least one permanently empty slot — required by
   * linear probing to terminate a failed search even when {@code f >= 1.0}.
   * Cast to {@code double} (not {@code float}) avoids precision loss on large tables.
   */
  private static int maxFill(final int n, final float f) {
    return Math.min((int) Math.ceil(n * (double) f), n - 1);
  }

  /**
   * Doubles the table capacity and rehashes all existing entries.
   *
   * <p>If capacity is already {@link #MAXIMUM_CAPACITY} ({@code 1 << 30}), sets
   * {@code threshold = Integer.MAX_VALUE} and returns without resizing.
   * ({@code (1<<30)<<1} would silently wrap to {@code Integer.MIN_VALUE} in Java,
   * causing {@code new int[Integer.MIN_VALUE]} to throw {@link NegativeArraySizeException}.)
   * The sentinel is safe because {@code size} is bounded by capacity.
   *
   * <p>Rehashing uses stored fingerprints ({@code slotHashes[i] & newMask}) directly —
   * {@link #getIndex} is not called, so subclass overrides do not affect placement after resize.
   */
  private void resize() {
    final int oldCapacity = this.capacity;

    // At MAXIMUM_CAPACITY we cannot double the array without overflowing a
    // signed 32-bit int.  Raise the threshold to inhibit future resize calls.
    if (oldCapacity >= MAXIMUM_CAPACITY) {
      this.threshold = Integer.MAX_VALUE;
      return;
    }

    final int[] oldHashTable = this.hashTable;
    final int[] oldSlotHashes = this.slotHashes;

    final int newCapacity = oldCapacity << 1;  // always <= MAXIMUM_CAPACITY, safe
    final int[] newHashTable = new int[newCapacity];
    final int[] newSlotHashes = new int[newCapacity];

    // Update state before rehash loop so this.mask reflects the new capacity.
    this.capacity = newCapacity;
    this.mask = newCapacity - 1;
    this.hashTable = newHashTable;
    this.slotHashes = newSlotHashes;
    // newCapacity <= MAXIMUM_CAPACITY = 1<<30, so maxFill always fits in int.
    this.threshold = maxFill(newCapacity, loadFactor);

    for (int i = 0; i < oldCapacity; i++) {
      if (oldHashTable[i] == EMPTY) {
        continue;
      }
      // Reuse stored fingerprint — no need to re-read key bytes or recompute hash.
      int newSlot = oldSlotHashes[i] & this.mask;
      while (newHashTable[newSlot] != EMPTY) {
        newSlot = (newSlot + 1) & this.mask;
      }
      newHashTable[newSlot] = oldHashTable[i];
      newSlotHashes[newSlot] = oldSlotHashes[i];
    }
  }
}
