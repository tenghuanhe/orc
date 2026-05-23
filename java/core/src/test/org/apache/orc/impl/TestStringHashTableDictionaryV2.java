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
import org.apache.orc.StringDictTestingUtils;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestStringHashTableDictionaryV2 {

  /**
   * Basic test using real FNV-1a hash. Traversal order is hash/slot-dependent
   * and is not asserted here; correctness is verified through position lookups.
   */
  @Test
  public void test0() throws Exception {
    StringHashTableDictionaryV2 htDict = new StringHashTableDictionaryV2(5);

    byte[] aliceBytes = new Text("Alice").getBytes();
    byte[] bobBytes   = new Text("Bob").getBytes();
    byte[] cindyBytes = new Text("Cindy").getBytes();
    byte[] davidBytes = new Text("David").getBytes();
    byte[] easonBytes = new Text("Eason").getBytes();

    // Initial state: hashTable and slotHashes are allocated (2 * capacity * 4 bytes);
    // byteArray, keyOffsets, and keyLengths have no chunks yet.
    // initialCapacity=5 → tableSizeFor(5)=8 → 2 * 8 * 4 = 64 bytes
    assertEquals(2L * 8 * Integer.BYTES, htDict.getSizeInBytes());
    assertEquals(0, htDict.add(aliceBytes, 0, aliceBytes.length));
    assertEquals(1, htDict.add(bobBytes,   0, bobBytes.length));
    assertEquals(0, htDict.add(aliceBytes, 0, aliceBytes.length)); // duplicate
    assertEquals(1, htDict.add(bobBytes,   0, bobBytes.length));   // duplicate
    assertEquals(2, htDict.add(cindyBytes, 0, cindyBytes.length));

    Text text = new Text();
    htDict.getText(text, 0);
    assertEquals("Alice", text.toString());
    htDict.getText(text, 1);
    assertEquals("Bob", text.toString());
    htDict.getText(text, 2);
    assertEquals("Cindy", text.toString());

    assertEquals(3, htDict.size());

    // The fourth and fifth elements — capacity=8, threshold=6, so no resize yet.
    assertEquals(3, htDict.add(davidBytes, 0, davidBytes.length));
    htDict.getText(text, 3);
    assertEquals("David", text.toString());
    assertEquals(4, htDict.add(easonBytes, 0, easonBytes.length));
    htDict.getText(text, 4);
    assertEquals("Eason", text.toString());

    assertEquals(5, htDict.size());

    // Re-verify all positions via getText.
    htDict.getText(text, 0);
    assertEquals("Alice", text.toString());
    htDict.getText(text, 1);
    assertEquals("Bob", text.toString());
    htDict.getText(text, 2);
    assertEquals("Cindy", text.toString());

    // Traversal order depends on FNV-1a slot placement and is an implementation
    // detail. Verify all entries are present with the correct original positions.
    Map<String, Integer> positions = new HashMap<>();
    htDict.visit(ctx -> positions.put(ctx.getText().toString(), ctx.getOriginalPosition()));
    assertEquals(Set.of("Alice", "Bob", "Cindy", "David", "Eason"), positions.keySet());
    assertEquals(0, (int) positions.get("Alice"));
    assertEquals(1, (int) positions.get("Bob"));
    assertEquals(2, (int) positions.get("Cindy"));
    assertEquals(3, (int) positions.get("David"));
    assertEquals(4, (int) positions.get("Eason"));

    htDict.clear();
    assertEquals(0, htDict.size());
  }

  /**
   * Extension of {@link StringHashTableDictionaryV2} for testing: overrides
   * {@link #getIndex} to return the numeric prefix of each key as the initial
   * slot. This makes traversal order deterministic and easy to reason about.
   */
  private static class SimpleHashDictionaryV2 extends StringHashTableDictionaryV2 {
    SimpleHashDictionaryV2(int initialCapacity) {
      super(initialCapacity);
    }

    /**
     * Returns the numeric prefix byte of the key as the slot index.
     * All keys used in {@link #test1} have a single-digit decimal prefix
     * (e.g. "0_David", "1_Cindy") so their initial slots are 0–4.
     */
    @Override
    int getIndex(byte[] bytes, int offset, int length, int hash) {
      return (char) bytes[offset] - '0';
    }
  }

  /**
   * Deterministic traversal test using {@link SimpleHashDictionaryV2}.
   *
   * <p>With {@code initialCapacity=5}, {@link StringHashTableDictionaryV2} rounds
   * up to the next power-of-two: {@code capacity=8}, {@code threshold=6}.
   * The overridden {@code getIndex} maps each key to its numeric prefix as its
   * initial slot (0–4), so all five entries are placed without collision and
   * no resize occurs. Traversal visits slots 0..7 in order, yielding a
   * deterministic sequence.
   *
   * <p>This test deliberately keeps the insertion count (5) below the resize
   * threshold (6). {@link StringHashTableDictionaryV2#resize()} bypasses
   * {@link StringHashTableDictionaryV2#getIndex} and reuses stored hash
   * fingerprints directly, so a subclass that derives slot positions from key
   * bytes would see inconsistent placement after a resize. See the
   * {@code getIndex} Javadoc for details.
   */
  @Test
  public void test1() throws Exception {
    SimpleHashDictionaryV2 hashTableDictionary = new SimpleHashDictionaryV2(5);

    assertEquals(2L * 8 * Integer.BYTES, hashTableDictionary.getSizeInBytes());
    assertEquals(0, hashTableDictionary.add(new Text("2_Alice")));
    assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));
    assertEquals(0, hashTableDictionary.add(new Text("2_Alice"))); // duplicate
    assertEquals(1, hashTableDictionary.add(new Text("3_Bob")));   // duplicate
    assertEquals(2, hashTableDictionary.add(new Text("1_Cindy")));

    Text text = new Text();
    hashTableDictionary.getText(text, 0);
    assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    assertEquals("1_Cindy", text.toString());

    assertEquals(3, hashTableDictionary.add(new Text("0_David")));
    hashTableDictionary.getText(text, 3);
    assertEquals("0_David", text.toString());
    assertEquals(4, hashTableDictionary.add(new Text("4_Eason")));
    hashTableDictionary.getText(text, 4);
    assertEquals("4_Eason", text.toString());

    // Re-verify previously inserted strings.
    hashTableDictionary.getText(text, 0);
    assertEquals("2_Alice", text.toString());
    hashTableDictionary.getText(text, 1);
    assertEquals("3_Bob", text.toString());
    hashTableDictionary.getText(text, 2);
    assertEquals("1_Cindy", text.toString());

    // Slots 0..4 are occupied by: 0_David(3), 1_Cindy(2), 2_Alice(0), 3_Bob(1), 4_Eason(4).
    StringDictTestingUtils.checkContents(
        hashTableDictionary,
        new int[]{3, 2, 0, 1, 4},
        "0_David", "1_Cindy", "2_Alice", "3_Bob", "4_Eason");

    hashTableDictionary.clear();
    assertEquals(0, hashTableDictionary.size());
  }

  // -------------------------------------------------------------------------
  // resize() coverage
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@code resize()} correctly rehashes all entries and every
   * key remains accessible at its original position after two consecutive
   * resizes.
   *
   * <p>With {@code initialCapacity=2}: capacity=2, threshold=1.
   * <ul>
   *   <li>add("apple")  – size=0 &lt; 1 → no resize; size→1</li>
   *   <li>add("banana") – size=1 ≥ 1 → resize → capacity=4, threshold=3; size→2</li>
   *   <li>add("cherry") – size=2 &lt; 3 → no resize; size→3</li>
   *   <li>add("date")   – size=3 ≥ 3 → resize → capacity=8, threshold=6; size→4</li>
   *   <li>add("fig")    – size=4 &lt; 6 → no resize; size→5</li>
   * </ul>
   */
  @Test
  public void testResizePreservesAllKeys() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(2);

    assertEquals(0, dict.add(new Text("apple")));
    assertEquals(1, dict.add(new Text("banana")));
    assertEquals(2, dict.add(new Text("cherry")));
    assertEquals(3, dict.add(new Text("date")));
    assertEquals(4, dict.add(new Text("fig")));
    assertEquals(5, dict.size());

    // All keys must be retrievable at their original positions after two resizes.
    Text t = new Text();
    dict.getText(t, 0); assertEquals("apple",  t.toString());
    dict.getText(t, 1); assertEquals("banana", t.toString());
    dict.getText(t, 2); assertEquals("cherry", t.toString());
    dict.getText(t, 3); assertEquals("date",   t.toString());
    dict.getText(t, 4); assertEquals("fig",    t.toString());

    // Duplicate detection must still work correctly after resize.
    assertEquals(0, dict.add(new Text("apple")));
    assertEquals(1, dict.add(new Text("banana")));
    assertEquals(2, dict.add(new Text("cherry")));
    assertEquals(3, dict.add(new Text("date")));
    assertEquals(4, dict.add(new Text("fig")));
    assertEquals(5, dict.size()); // no growth
  }

  // -------------------------------------------------------------------------
  // Linear probing and fingerprint fast-rejection coverage
  // -------------------------------------------------------------------------

  /**
   * Extension that forces all keys to start probing at slot 0, creating a
   * deterministic left-to-right collision chain.
   *
   * <p>Keep insertions below the resize threshold so that the overridden
   * {@code getIndex} is never bypassed by
   * {@link StringHashTableDictionaryV2#resize()}.
   */
  private static class AllSlotZeroDictionary extends StringHashTableDictionaryV2 {
    AllSlotZeroDictionary(int initialCapacity) {
      super(initialCapacity);
    }

    @Override
    int getIndex(byte[] bytes, int offset, int length, int hash) {
      return 0;
    }
  }

  /**
   * Verifies linear probing and FNV-1a fingerprint fast-rejection.
   *
   * <p>All insertions start at slot 0 ({@link AllSlotZeroDictionary}), creating
   * the chain: "first"→slot 0, "second"→slot 1, "third"→slot 2.
   *
   * <p>Duplicate lookups exercise the full probe chain:
   * <ul>
   *   <li>"first" duplicate – fingerprint match at slot 0 (direct hit)</li>
   *   <li>"second" duplicate – fingerprint mismatch at slot 0 → probe → hit slot 1</li>
   *   <li>"third" duplicate – fingerprint mismatches at slots 0,1 → probe → hit slot 2</li>
   * </ul>
   */
  @Test
  public void testLinearProbingAndFingerprintRejection() throws Exception {
    // capacity=8, threshold=6; keep size ≤ 5 to stay below the threshold.
    AllSlotZeroDictionary dict = new AllSlotZeroDictionary(8);

    assertEquals(0, dict.add(new Text("first")));
    assertEquals(1, dict.add(new Text("second")));
    assertEquals(2, dict.add(new Text("third")));
    assertEquals(3, dict.size());

    // Duplicate detection through the full probe chain.
    assertEquals(0, dict.add(new Text("first")));   // direct hit at slot 0
    assertEquals(1, dict.add(new Text("second")));  // skip slot 0 (fingerprint mismatch), hit slot 1
    assertEquals(2, dict.add(new Text("third")));   // skip slots 0,1, hit slot 2
    assertEquals(3, dict.size());                   // no new entries

    // Traversal order: slots 0→1→2 hold "first"(0), "second"(1), "third"(2).
    StringDictTestingUtils.checkContents(dict, new int[]{0, 1, 2}, "first", "second", "third");
  }

  // -------------------------------------------------------------------------
  // getText(int) / writeTo() coverage
  // -------------------------------------------------------------------------

  /**
   * Tests the {@link StringHashTableDictionaryV2#getText(int)} overload that
   * returns a {@link ByteBuffer}.
   */
  @Test
  public void testGetTextByteBuffer() {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4);
    dict.add(new Text("hello"));
    dict.add(new Text("world"));

    ByteBuffer buf0 = dict.getText(0);
    byte[] bytes0 = new byte[buf0.remaining()];
    buf0.get(bytes0);
    assertEquals("hello", new String(bytes0, StandardCharsets.UTF_8));

    ByteBuffer buf1 = dict.getText(1);
    byte[] bytes1 = new byte[buf1.remaining()];
    buf1.get(bytes1);
    assertEquals("world", new String(bytes1, StandardCharsets.UTF_8));
  }

  /**
   * Tests {@link StringHashTableDictionaryV2#writeTo(java.io.OutputStream, int)}.
   */
  @Test
  public void testWriteTo() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4);
    dict.add(new Text("hello"));
    dict.add(new Text("world"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    int len0 = dict.writeTo(baos, 0);
    assertEquals("hello".length(), len0);
    assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), baos.toByteArray());

    baos.reset();
    int len1 = dict.writeTo(baos, 1);
    assertEquals("world".length(), len1);
    assertArrayEquals("world".getBytes(StandardCharsets.UTF_8), baos.toByteArray());
  }

  // -------------------------------------------------------------------------
  // Edge cases: empty string, byte offset, small capacity, load factor
  // -------------------------------------------------------------------------

  /**
   * Verifies that an empty (zero-length) key is stored, retrieved, and
   * de-duplicated correctly.
   */
  @Test
  public void testEmptyString() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4);
    byte[] empty = new byte[0];

    assertEquals(0, dict.add(empty, 0, 0));
    assertEquals(0, dict.add(empty, 0, 0)); // duplicate
    assertEquals(1, dict.size());

    Text t = new Text();
    dict.getText(t, 0);
    assertEquals("", t.toString());

    ByteBuffer buf = dict.getText(0);
    assertEquals(0, buf.remaining());
  }

  /**
   * Verifies that {@code add(byte[], offset, length)} with a non-zero
   * {@code offset} correctly identifies duplicates regardless of the buffer
   * position the bytes reside in.
   */
  @Test
  public void testAddBytesWithNonZeroOffset() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4);

    byte[] buf1 = "XhelloX".getBytes(StandardCharsets.UTF_8);
    byte[] buf2 = "YhelloY".getBytes(StandardCharsets.UTF_8);

    assertEquals(0, dict.add(buf1, 1, 5)); // "hello" from offset 1
    assertEquals(0, dict.add(buf2, 1, 5)); // same content → duplicate
    assertEquals(1, dict.size());

    Text t = new Text();
    dict.getText(t, 0);
    assertEquals("hello", t.toString());
  }

  /**
   * Verifies that {@code initialCapacity} values of 0 and 1 both produce a
   * starting table capacity of 1 (via {@code tableSizeFor}), and that the
   * table grows on-demand as keys are inserted.
   */
  @Test
  public void testSmallInitialCapacity() throws Exception {
    for (int initCap : new int[]{0, 1}) {
      StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(initCap);
      // capacity=1; hashTable[1] + slotHashes[1] = 2*1*4 = 8 bytes
      assertEquals(2L * 1 * Integer.BYTES, dict.getSizeInBytes());

      // threshold=0, so every add triggers resize until stable
      assertEquals(0, dict.add(new Text("x")));
      assertEquals(1, dict.add(new Text("y")));
      assertEquals(2, dict.add(new Text("z")));
      assertEquals(3, dict.size());

      Text t = new Text();
      dict.getText(t, 0); assertEquals("x", t.toString());
      dict.getText(t, 1); assertEquals("y", t.toString());
      dict.getText(t, 2); assertEquals("z", t.toString());

      // Duplicate detection must work after multiple resizes.
      assertEquals(0, dict.add(new Text("x")));
      assertEquals(1, dict.add(new Text("y")));
      assertEquals(2, dict.add(new Text("z")));
      assertEquals(3, dict.size());
    }
  }

  /**
   * Verifies that a custom load factor controls when resize is triggered.
   *
   * <p>With {@code initialCapacity=4} and {@code loadFactor=0.5}:
   * capacity=4, threshold=2. The third insertion triggers resize.
   */
  @Test
  public void testCustomLoadFactor() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 0.5f);
    // threshold = (int)(4 * 0.5) = 2

    assertEquals(0, dict.add(new Text("alpha")));
    assertEquals(1, dict.add(new Text("beta")));
    // size=2 ≥ threshold=2 → resize triggered on next add
    assertEquals(2, dict.add(new Text("gamma")));
    assertEquals(3, dict.size());

    Text t = new Text();
    dict.getText(t, 0); assertEquals("alpha", t.toString());
    dict.getText(t, 1); assertEquals("beta",  t.toString());
    dict.getText(t, 2); assertEquals("gamma", t.toString());

    // Duplicate detection works after resize.
    assertEquals(0, dict.add(new Text("alpha")));
    assertEquals(1, dict.add(new Text("beta")));
    assertEquals(2, dict.add(new Text("gamma")));
    assertEquals(3, dict.size());
  }

  // -------------------------------------------------------------------------
  // clear() + reuse coverage
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@link StringHashTableDictionaryV2#clear()} resets the
   * logical state (size, key data) without shrinking the hash-table arrays,
   * and that subsequent insertions build a fresh dictionary from position 0.
   *
   * <p>Also confirms that keys present before {@code clear()} are no longer
   * found after it, and are re-inserted as new entries if added again.
   */
  @Test
  public void testClearAndReuseAfterResize() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(2);

    // Trigger at least one resize so the post-clear capacity is larger than initial.
    assertEquals(0, dict.add(new Text("one")));
    assertEquals(1, dict.add(new Text("two")));
    assertEquals(2, dict.add(new Text("three")));
    assertEquals(3, dict.size());

    dict.clear();
    assertEquals(0, dict.size());
    // Hash-table arrays are kept (capacity stays doubled); dynamic key arrays
    // are freed, so only the flat int[] contribute to the in-memory footprint.
    assertTrue(dict.getSizeInBytes() > 0);

    // Re-add different items; positions restart from 0.
    assertEquals(0, dict.add(new Text("alpha")));
    assertEquals(1, dict.add(new Text("beta")));
    assertEquals(2, dict.add(new Text("gamma")));
    assertEquals(3, dict.size());

    Text t = new Text();
    dict.getText(t, 0); assertEquals("alpha", t.toString());
    dict.getText(t, 1); assertEquals("beta",  t.toString());
    dict.getText(t, 2); assertEquals("gamma", t.toString());

    // The pre-clear strings are gone; re-adding them yields new positions.
    assertEquals(3, dict.add(new Text("one")));
    assertEquals(4, dict.add(new Text("two")));
    assertEquals(5, dict.add(new Text("three")));
    assertEquals(6, dict.size());
  }

  // -------------------------------------------------------------------------
  // getSizeInBytes() growth coverage
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@link StringHashTableDictionaryV2#getSizeInBytes()} grows
   * after keys are inserted, reflecting the allocation of dynamic byte and
   * int arrays for key storage.
   */
  @Test
  public void testSizeInBytesGrowsAfterAdd() {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(8);
    long initialSize = dict.getSizeInBytes();
    // capacity=8; only hashTable + slotHashes contribute initially.
    assertEquals(2L * 8 * Integer.BYTES, initialSize);

    dict.add(new Text("hello"));
    dict.add(new Text("world"));

    // After insertions, byteArray, keyOffsets, and keyLengths have allocated
    // chunks, so the total must exceed the initial hash-table-only footprint.
    assertTrue(dict.getSizeInBytes() > initialSize);
  }

  // -------------------------------------------------------------------------
  // Resize overflow boundary (MAXIMUM_CAPACITY guard)
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@code tableSizeFor} never returns a value larger than
   * {@code MAXIMUM_CAPACITY (1 << 30)}, even for extremely large inputs,
   * so that a subsequent left-shift in {@code resize()} cannot overflow.
   *
   * <p>Uses reflection to call the private static method directly.
   */
  @Test
  public void testTableSizeForCapsAtMaximumCapacity() throws Exception {
    final int maximumCapacity = 1 << 30;

    java.lang.reflect.Method m =
        StringHashTableDictionaryV2.class.getDeclaredMethod("tableSizeFor", int.class);
    m.setAccessible(true);

    // Values well beyond MAXIMUM_CAPACITY must be capped.
    for (int n : new int[]{maximumCapacity, maximumCapacity + 1,
                           Integer.MAX_VALUE - 1, Integer.MAX_VALUE}) {
      int result = (int) m.invoke(null, n);
      assertEquals(maximumCapacity, result,
          "tableSizeFor(" + n + ") should be capped at MAXIMUM_CAPACITY");
    }

    // A non-power-of-two just above 2^29 must be rounded up to 2^30.
    assertEquals(maximumCapacity, (int) m.invoke(null, (1 << 29) + 1));
    // An exact power-of-two (2^29) must be returned unchanged.
    assertEquals(1 << 29,         (int) m.invoke(null, 1 << 29));
  }

  /**
   * Verifies that {@code resize()} throws {@link OutOfMemoryError} when the
   * capacity has already reached {@code MAXIMUM_CAPACITY} (1 &lt;&lt; 30).
   * Doubling such a capacity would overflow a signed 32-bit integer and produce
   * a negative array size; fail-fast with an unrecoverable error is safer than
   * silently degrading (which risks an infinite loop once every slot is occupied).
   *
   * <p>Uses reflection to inject {@code capacity = MAXIMUM_CAPACITY} and call
   * {@code resize()} directly, avoiding any real multi-gigabyte allocation.
   */
  @Test
  public void testResizeThrowsOutOfMemoryErrorAtMaximumCapacity() throws Exception {
    final int maximumCapacity = 1 << 30;

    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 1.0f);

    // Forge capacity = MAXIMUM_CAPACITY so that resize() observes the boundary.
    java.lang.reflect.Field capField =
        StringHashTableDictionaryV2.class.getDeclaredField("capacity");
    capField.setAccessible(true);
    capField.setInt(dict, maximumCapacity);

    java.lang.reflect.Method resizeMethod =
        StringHashTableDictionaryV2.class.getDeclaredMethod("resize");
    resizeMethod.setAccessible(true);

    // resize() must throw OutOfMemoryError (wrapped by reflection as InvocationTargetException).
    try {
      resizeMethod.invoke(dict);
      fail("Expected OutOfMemoryError when capacity == MAXIMUM_CAPACITY");
    } catch (java.lang.reflect.InvocationTargetException ite) {
      assertTrue(ite.getCause() instanceof OutOfMemoryError,
          "Expected OutOfMemoryError cause, got: " + ite.getCause());
    }
  }
}
