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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestStringHashTableDictionaryV2 {

  /** Basic add/get/dedup/visit/clear using the real FNV-1a hash. */
  @Test
  public void test0() throws Exception {
    StringHashTableDictionaryV2 htDict = new StringHashTableDictionaryV2(5);

    byte[] aliceBytes = new Text("Alice").getBytes();
    byte[] bobBytes   = new Text("Bob").getBytes();
    byte[] cindyBytes = new Text("Cindy").getBytes();
    byte[] davidBytes = new Text("David").getBytes();
    byte[] easonBytes = new Text("Eason").getBytes();

    // initialCapacity=5 → capacity=8; only hashTable+slotHashes allocated → 2*8*4=64 bytes
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

    // capacity=8, threshold=6: no resize before the 6th distinct key
    assertEquals(3, htDict.add(davidBytes, 0, davidBytes.length));
    htDict.getText(text, 3);
    assertEquals("David", text.toString());
    assertEquals(4, htDict.add(easonBytes, 0, easonBytes.length));
    htDict.getText(text, 4);
    assertEquals("Eason", text.toString());

    assertEquals(5, htDict.size());

    htDict.getText(text, 0);
    assertEquals("Alice", text.toString());
    htDict.getText(text, 1);
    assertEquals("Bob", text.toString());
    htDict.getText(text, 2);
    assertEquals("Cindy", text.toString());

    // Traversal order is slot-dependent; verify presence and correct original positions.
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
   * Overrides {@code getIndex} to use the key's numeric prefix byte as the
   * initial slot, making traversal order deterministic.
   */
  private static class SimpleHashDictionaryV2 extends StringHashTableDictionaryV2 {
    SimpleHashDictionaryV2(int initialCapacity) {
      super(initialCapacity);
    }

    /** Maps key "N_Name" to slot N via the leading ASCII digit. */
    @Override
    int getIndex(byte[] bytes, int offset, int length, int hash) {
      return (char) bytes[offset] - '0';
    }
  }

  /**
   * Deterministic traversal test using {@link SimpleHashDictionaryV2}.
   * capacity=8, threshold=6; keys "0_David"–"4_Eason" land at slots 0–4
   * without collision and without a resize, so visit order is predictable.
   *
   * <p>Size is kept below the resize threshold intentionally: {@code resize()}
   * bypasses {@code getIndex} and uses stored fingerprints directly, so a
   * subclass overriding slot placement would see inconsistent positions after
   * a rehash.
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
   * Verifies that all keys remain accessible at their original positions after
   * two consecutive resizes.
   *
   * <p>initialCapacity=2, threshold=1:
   * <ul>
   *   <li>"banana" triggers the 1st resize → capacity=4, threshold=3</li>
   *   <li>"date"   triggers the 2nd resize → capacity=8, threshold=6</li>
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

    // All positions stable after two resizes.
    Text t = new Text();
    dict.getText(t, 0); assertEquals("apple",  t.toString());
    dict.getText(t, 1); assertEquals("banana", t.toString());
    dict.getText(t, 2); assertEquals("cherry", t.toString());
    dict.getText(t, 3); assertEquals("date",   t.toString());
    dict.getText(t, 4); assertEquals("fig",    t.toString());

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
   * Forces all keys to slot 0 for deterministic left-to-right collision-chain
   * testing. Keep size below the resize threshold — {@code resize()} bypasses
   * {@code getIndex} when rehashing.
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
   * Verifies linear probing and FNV-1a fingerprint fast-rejection using
   * {@link AllSlotZeroDictionary}: all keys start at slot 0, so each new
   * key probes past all earlier ones.
   */
  @Test
  public void testLinearProbingAndFingerprintRejection() throws Exception {
    // capacity=8, threshold=6; keep size ≤ 3 to stay well below the threshold.
    AllSlotZeroDictionary dict = new AllSlotZeroDictionary(8);

    assertEquals(0, dict.add(new Text("first")));
    assertEquals(1, dict.add(new Text("second")));
    assertEquals(2, dict.add(new Text("third")));
    assertEquals(3, dict.size());

    // Duplicate detection through the full probe chain.
    assertEquals(0, dict.add(new Text("first")));   // fingerprint match at slot 0
    assertEquals(1, dict.add(new Text("second")));  // mismatch at slot 0 → probe → slot 1
    assertEquals(2, dict.add(new Text("third")));   // mismatches at slots 0,1 → slot 2
    assertEquals(3, dict.size());                   // no new entries

    // Traversal order: slots 0→1→2 hold "first"(0), "second"(1), "third"(2).
    StringDictTestingUtils.checkContents(dict, new int[]{0, 1, 2}, "first", "second", "third");
  }

  // -------------------------------------------------------------------------
  // getText(int) / writeTo() coverage
  // -------------------------------------------------------------------------

  /** Tests the {@code getText(int)} overload that returns a {@link ByteBuffer}. */
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

  /** Tests {@code writeTo(OutputStream, int)}. */
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

  /** Verifies that a zero-length key is stored, retrieved, and de-duplicated correctly. */
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

  /** Verifies duplicate detection when the same key bytes reside at different buffer offsets. */
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
   * Verifies that initialCapacity=0 and =1 both produce capacity=1, and the
   * table grows on demand.
   */
  @Test
  public void testSmallInitialCapacity() throws Exception {
    for (int initCap : new int[]{0, 1}) {
      StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(initCap);
      // capacity=1; hashTable[1] + slotHashes[1] = 2*1*4 = 8 bytes
      assertEquals(2L * 1 * Integer.BYTES, dict.getSizeInBytes());

      // threshold=0, so every add triggers a resize
      assertEquals(0, dict.add(new Text("x")));
      assertEquals(1, dict.add(new Text("y")));
      assertEquals(2, dict.add(new Text("z")));
      assertEquals(3, dict.size());

      Text t = new Text();
      dict.getText(t, 0); assertEquals("x", t.toString());
      dict.getText(t, 1); assertEquals("y", t.toString());
      dict.getText(t, 2); assertEquals("z", t.toString());

      assertEquals(0, dict.add(new Text("x")));
      assertEquals(1, dict.add(new Text("y")));
      assertEquals(2, dict.add(new Text("z")));
      assertEquals(3, dict.size());
    }
  }

  /**
   * Verifies that a custom load factor controls when resize fires.
   * initialCapacity=4, loadFactor=0.5: threshold=2, so the 3rd insertion triggers resize.
   */
  @Test
  public void testCustomLoadFactor() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 0.5f);
    // threshold = min(ceil(4*0.5), 3) = 2

    assertEquals(0, dict.add(new Text("alpha")));
    assertEquals(1, dict.add(new Text("beta")));
    // size=2 ≥ threshold=2 → resize triggered before the next add
    assertEquals(2, dict.add(new Text("gamma")));
    assertEquals(3, dict.size());

    Text t = new Text();
    dict.getText(t, 0); assertEquals("alpha", t.toString());
    dict.getText(t, 1); assertEquals("beta",  t.toString());
    dict.getText(t, 2); assertEquals("gamma", t.toString());

    assertEquals(0, dict.add(new Text("alpha")));
    assertEquals(1, dict.add(new Text("beta")));
    assertEquals(2, dict.add(new Text("gamma")));
    assertEquals(3, dict.size());
  }

  // -------------------------------------------------------------------------
  // clear() + reuse
  // -------------------------------------------------------------------------

  /**
   * Verifies that {@code clear()} resets size and key data without shrinking
   * the hash-table arrays, and that re-insertion starts positions from 0.
   */
  @Test
  public void testClearAndReuseAfterResize() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(2);

    // trigger a resize
    assertEquals(0, dict.add(new Text("one")));
    assertEquals(1, dict.add(new Text("two")));
    assertEquals(2, dict.add(new Text("three")));
    assertEquals(3, dict.size());

    dict.clear();
    assertEquals(0, dict.size());
    // hash-table arrays retained; key storage freed → only flat int[] contribute
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
  // maxFill safety: at least one empty slot guaranteed (fastutil pattern)
  // -------------------------------------------------------------------------

  /**
   * Verifies the fastutil {@code maxFill} invariant: even with a load factor
   * of 1.0, the threshold is capped at {@code capacity - 1}, guaranteeing
   * at least one permanently empty slot.  Without this cap, filling every
   * slot and then looking up a missing key would cause an infinite probe loop.
   *
   * <p>With {@code loadFactor = 1.0} and {@code initialCapacity = 4}:
   * capacity = 4, threshold must be {@code min(ceil(4 * 1.0), 3) = 3}.
   * The table still resizes once the 3rd element is inserted, keeping the
   * probe loop safe.
   */
  @Test
  public void testLoadFactorOneDoesNotFillAllSlots() throws Exception {
    // loadFactor=1.0: without the n-1 cap, threshold would equal capacity,
    // allowing all slots to be occupied and causing an infinite probe loop.
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 1.0f);

    // Insert many distinct keys — none must cause an infinite loop.
    final int COUNT = 200;
    for (int i = 0; i < COUNT; i++) {
      assertEquals(i, dict.add(new Text("key-" + i)));
    }
    assertEquals(COUNT, dict.size());

    // All keys must be retrievable.
    Text t = new Text();
    for (int i = 0; i < COUNT; i++) {
      dict.getText(t, i);
      assertEquals("key-" + i, t.toString());
    }

    // Duplicate detection must still work.
    for (int i = 0; i < COUNT; i++) {
      assertEquals(i, dict.add(new Text("key-" + i)));
    }
    assertEquals(COUNT, dict.size());
  }

  // -------------------------------------------------------------------------
  // Resize overflow boundary
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
   * Verifies that {@code resize()} handles the {@code MAXIMUM_CAPACITY} boundary
   * gracefully: instead of attempting to allocate a negative-sized array
   * (which would throw {@link NegativeArraySizeException}), it raises the
   * threshold to {@link Integer#MAX_VALUE} to permanently suppress further
   * resize attempts — mirroring {@code java.util.HashMap}'s behaviour.
   *
   * <p>Uses reflection to inject {@code capacity = MAXIMUM_CAPACITY} and call
   * {@code resize()} directly, avoiding any real multi-gigabyte allocation.
   */
  @Test
  public void testResizeAtMaximumCapacityRaisesThreshold() throws Exception {
    final int maximumCapacity = 1 << 30;

    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 1.0f);

    // Forge capacity = MAXIMUM_CAPACITY so that resize() observes the boundary.
    java.lang.reflect.Field capField =
        StringHashTableDictionaryV2.class.getDeclaredField("capacity");
    capField.setAccessible(true);
    capField.setInt(dict, maximumCapacity);

    java.lang.reflect.Field threshField =
        StringHashTableDictionaryV2.class.getDeclaredField("threshold");
    threshField.setAccessible(true);

    java.lang.reflect.Method resizeMethod =
        StringHashTableDictionaryV2.class.getDeclaredMethod("resize");
    resizeMethod.setAccessible(true);

    // resize() must NOT throw; it should set threshold = Integer.MAX_VALUE.
    resizeMethod.invoke(dict);

    assertEquals(Integer.MAX_VALUE, threshField.getInt(dict),
        "resize() at MAXIMUM_CAPACITY must set threshold = Integer.MAX_VALUE");

    // capacity must remain unchanged (no reallocation occurred).
    assertEquals(maximumCapacity, capField.getInt(dict),
        "capacity must remain MAXIMUM_CAPACITY after no-op resize");
  }

  // ==========================================================================
  // Boundary condition tests (fastutil review + correctness invariants)
  // ==========================================================================

  /**
   * Verifies {@code maxFill(n, f)} directly: result must equal
   * {@code min(ceil(n * f), n - 1)} for a representative set of inputs.
   *
   * <p>The {@code n - 1} cap is the critical safety property: linear probing
   * requires at least one permanently empty slot so that the probe loop
   * terminates even for keys not present in the table.
   */
  @Test
  public void testMaxFillFormulaViaReflection() throws Exception {
    java.lang.reflect.Method maxFillMethod =
        StringHashTableDictionaryV2.class.getDeclaredMethod("maxFill", int.class, float.class);
    maxFillMethod.setAccessible(true);

    // { n, f-bits-as-int, expected }
    // f encoded as raw bits to avoid ambiguous literal widening
    Object[][] cases = {
        {  1, 0.75f,  0 },  // ceil(0.75)=1, but n-1=0 dominates
        {  2, 0.75f,  1 },  // ceil(1.5)=2,  n-1=1 dominates
        {  4, 0.75f,  3 },  // ceil(3.0)=3,  n-1=3 → min(3,3)=3
        {  8, 0.75f,  6 },  // ceil(6.0)=6,  n-1=7 → 6
        { 16, 0.75f, 12 },  // ceil(12.0)=12, n-1=15 → 12
        {  4, 1.0f,   3 },  // ceil(4.0)=4,  n-1=3 dominates
        {  4, 1.5f,   3 },  // ceil(6.0)=6,  n-1=3 dominates
        {  8, 0.99f,  7 },  // ceil(7.92)=8, n-1=7 dominates
        {  8, 1.0f,   7 },  // ceil(8.0)=8,  n-1=7 dominates
        {  8, 2.0f,   7 },  // ceil(16.0)=16, n-1=7 dominates
    };

    for (Object[] c : cases) {
      int n = (int) c[0]; float f = (float) c[1]; int expected = (int) c[2];
      int result = (int) maxFillMethod.invoke(null, n, f);
      assertEquals(expected, result,
          "maxFill(" + n + ", " + f + ") should be " + expected);
      // Critical invariant: at least one slot must remain permanently empty
      assertTrue(result < n,
          "maxFill must be strictly < n, but got result=" + result + " for n=" + n);
    }
  }

  /**
   * Verifies the {@code threshold < capacity} invariant after construction and
   * after every resize, for several load-factor values including pathological
   * ones (&ge; 1.0).
   *
   * <p>Without this invariant the probe loop in {@code add()} could spin forever
   * when every slot is occupied and the target key is absent.
   */
  @Test
  public void testThresholdAlwaysLessThanCapacity() throws Exception {
    java.lang.reflect.Field threshField =
        StringHashTableDictionaryV2.class.getDeclaredField("threshold");
    threshField.setAccessible(true);
    java.lang.reflect.Field capField =
        StringHashTableDictionaryV2.class.getDeclaredField("capacity");
    capField.setAccessible(true);

    final int MAXIMUM_CAPACITY = 1 << 30;
    for (float lf : new float[]{ 0.5f, 0.75f, 0.99f, 1.0f, 1.5f }) {
      StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, lf);
      for (int i = 0; i < 200; i++) {
        byte[] key = ("inv-lf" + lf + "-" + i).getBytes(StandardCharsets.UTF_8);
        dict.add(key, 0, key.length);
        int thresh = threshField.getInt(dict);
        int cap    = capField.getInt(dict);
        // When MAXIMUM_CAPACITY is reached, threshold = Integer.MAX_VALUE (sentinel).
        // Otherwise threshold must be strictly < capacity.
        if (cap < MAXIMUM_CAPACITY) {
          assertTrue(thresh < cap,
              "threshold=" + thresh + " must be < capacity=" + cap
                  + " (loadFactor=" + lf + ", i=" + i + ")");
        }
      }
    }
  }

  /**
   * Verifies that a load factor &gt; 1.0 is handled safely end-to-end.
   * The {@code n-1} cap in {@code maxFill()} ensures {@code threshold < capacity}
   * even when {@code loadFactor &ge; 1.0}, preventing infinite probe loops.
   */
  @Test
  public void testLoadFactorGreaterThanOneIsSafe() throws Exception {
    // capacity=4, lf=2.0: threshold = min(ceil(8.0), 3) = 3
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(4, 2.0f);

    final int COUNT = 200;
    Map<String, Integer> keyToIndex = new HashMap<>();
    for (int i = 0; i < COUNT; i++) {
      String key = "extreme-lf-" + i;
      byte[] b = key.getBytes(StandardCharsets.UTF_8);
      int idx = dict.add(b, 0, b.length);
      assertTrue(idx >= 0, "add() must return a non-negative index");
      keyToIndex.put(key, idx);
    }

    assertEquals(COUNT, dict.size(), "size() must equal COUNT");
    assertEquals(COUNT, new HashSet<>(keyToIndex.values()).size(),
        "all keys must receive unique indices");

    // Every key must be retrievable at its stored index
    Text text = new Text();
    for (Map.Entry<String, Integer> entry : keyToIndex.entrySet()) {
      dict.getText(text, entry.getValue());
      assertEquals(entry.getKey(), text.toString(),
          "getText at index " + entry.getValue() + " must match");
    }

    // Duplicate detection must still work
    for (int i = 0; i < COUNT; i++) {
      String key = "extreme-lf-" + i;
      byte[] b = key.getBytes(StandardCharsets.UTF_8);
      assertEquals(keyToIndex.get(key).intValue(), dict.add(b, 0, b.length),
          "duplicate add must return original index for " + key);
    }
  }

  /**
   * Tests {@code tableSizeFor()} for small, zero, and negative inputs that
   * exercise the early-return {@code n &le; 1} branch.
   */
  @Test
  public void testTableSizeForSmallAndNegativeValues() throws Exception {
    java.lang.reflect.Method tsf =
        StringHashTableDictionaryV2.class.getDeclaredMethod("tableSizeFor", int.class);
    tsf.setAccessible(true);

    int[][] cases = {
        { Integer.MIN_VALUE, 1 },  // treated as <= 1
        { -100, 1 },
        { -1,   1 },
        { 0,    1 },
        { 1,    1 },
        { 2,    2 },
        { 3,    4 },
        { 4,    4 },
        { 5,    8 },
        { 7,    8 },
        { 8,    8 },
        { 9,   16 },
        { (1 << 29),      1 << 29 },  // exact power of two
        { (1 << 29) + 1,  1 << 30 },  // next power of two = MAXIMUM_CAPACITY
    };

    for (int[] c : cases) {
      int result = (int) tsf.invoke(null, c[0]);
      assertEquals(c[1], result, "tableSizeFor(" + c[0] + ")");
    }
  }

  /**
   * Verifies that a negative {@code initialCapacity} is silently treated as
   * &le; 1, producing a capacity-1 dictionary that grows on demand.
   */
  @Test
  public void testNegativeInitialCapacity() throws Exception {
    java.lang.reflect.Field capField =
        StringHashTableDictionaryV2.class.getDeclaredField("capacity");
    capField.setAccessible(true);

    for (int bad : new int[]{ -1, -100, Integer.MIN_VALUE }) {
      StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(bad);
      assertEquals(1, capField.getInt(dict),
          "capacity should be 1 for initialCapacity=" + bad);

      // The dictionary must still be fully usable
      byte[] hello = "hello".getBytes(StandardCharsets.UTF_8);
      byte[] world = "world".getBytes(StandardCharsets.UTF_8);
      int idx0 = dict.add(hello, 0, hello.length);
      int idx1 = dict.add(world, 0, world.length);
      assertEquals(0, idx0);
      assertEquals(1, idx1);
      assertEquals(0, dict.add(hello, 0, hello.length), "duplicate must return idx0");
      assertEquals(2, dict.size());

      Text text = new Text();
      dict.getText(text, idx0);
      assertEquals("hello", text.toString());
      dict.getText(text, idx1);
      assertEquals("world", text.toString());
    }
  }

  /**
   * Verifies that {@code visit()} traverses exactly {@code size()} unique entries
   * with no duplicates, and that {@code getOriginalPosition()} covers the
   * contiguous range {@code [0, size - 1]}.
   */
  @Test
  public void testVisitCountMatchesSize() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(8);

    final int N = 100;
    for (int i = 0; i < N; i++) {
      byte[] key = ("visit-key-" + i).getBytes(StandardCharsets.UTF_8);
      dict.add(key, 0, key.length);
    }
    assertEquals(N, dict.size());

    Set<Integer> positions = new HashSet<>();
    dict.visit(ctx -> positions.add(ctx.getOriginalPosition()));

    assertEquals(N, positions.size(),
        "visit() must traverse exactly size() unique entries");
    assertEquals(0, Collections.min(positions),
        "smallest position must be 0");
    assertEquals(N - 1, Collections.max(positions),
        "largest position must be size - 1");
  }

  /**
   * Verifies that {@code fnvHash()} never returns the {@code EMPTY} sentinel
   * (0), even for inputs whose FNV-1a computation would naturally yield 0.
   * The implementation replaces 0 with 1 to keep the sentinel unambiguous.
   */
  @Test
  public void testFnvHashNeverReturnsZero() throws Exception {
    java.lang.reflect.Method fnvHash =
        StringHashTableDictionaryV2.class.getDeclaredMethod(
            "fnvHash", byte[].class, int.class, int.class);
    fnvHash.setAccessible(true);

    // Empty string: FNV-1a offset basis (0x811c9dc5) is non-zero
    byte[] empty = new byte[0];
    int h = (int) fnvHash.invoke(null, empty, 0, 0);
    assertTrue(h != 0, "fnvHash of empty string must not be 0");

    // All 256 possible single-byte inputs
    for (int b = 0; b < 256; b++) {
      byte[] oneByte = new byte[]{ (byte) b };
      int hash = (int) fnvHash.invoke(null, oneByte, 0, 1);
      assertTrue(hash != 0,
          "fnvHash must not return 0 for single-byte input 0x" + Integer.toHexString(b));
    }

    // All 256 complement pairs that maximally stress XOR cancellation
    for (int b = 0; b < 256; b++) {
      byte[] pair = new byte[]{ (byte) b, (byte) (b ^ 0xFF) };
      int hash = (int) fnvHash.invoke(null, pair, 0, 2);
      assertTrue(hash != 0,
          "fnvHash must not return 0 for pair [" + b + ", " + (b ^ 0xFF) + "]");
    }
  }

  /**
   * Tests that multi-byte UTF-8 strings (Chinese, Japanese, Korean, emoji,
   * and mixed ASCII/multi-byte) are stored and retrieved correctly.
   */
  @Test
  public void testMultiByteUtf8Keys() throws Exception {
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(8);

    String[] keys = {
        "你好世界",           // Chinese:  4 chars, 12 UTF-8 bytes
        "日本語",             // Japanese: 3 chars,  9 UTF-8 bytes
        "한국어",             // Korean:   3 chars,  9 UTF-8 bytes
        "\uD83D\uDE00",      // U+1F600 emoji: 4-byte UTF-8 sequence
        "mix-混合-abc",       // mixed ASCII + CJK
        "",                  // empty string (boundary)
        "ASCII only",        // pure ASCII (regression guard)
    };

    int[] indices = new int[keys.length];
    for (int i = 0; i < keys.length; i++) {
      byte[] b = keys[i].getBytes(StandardCharsets.UTF_8);
      indices[i] = dict.add(b, 0, b.length);
    }
    assertEquals(keys.length, dict.size(), "all keys are distinct");

    // Duplicate detection
    for (int i = 0; i < keys.length; i++) {
      byte[] b = keys[i].getBytes(StandardCharsets.UTF_8);
      assertEquals(indices[i], dict.add(b, 0, b.length),
          "duplicate '" + keys[i] + "' must return same index");
    }

    // Retrieval via getText(Text, int)
    Text text = new Text();
    for (int i = 0; i < keys.length; i++) {
      dict.getText(text, indices[i]);
      assertEquals(keys[i], text.toString(),
          "getText(Text, " + indices[i] + ") mismatch for '" + keys[i] + "'");
    }

    // Retrieval via getText(int) returning ByteBuffer
    for (int i = 0; i < keys.length; i++) {
      ByteBuffer buf = dict.getText(indices[i]);
      String retrieved = StandardCharsets.UTF_8.decode(buf).toString();
      assertEquals(keys[i], retrieved,
          "getText(ByteBuffer, " + indices[i] + ") mismatch for '" + keys[i] + "'");
    }
  }

  /**
   * After {@code resize()} has expanded the table, {@code clear()} must
   * preserve the expanded capacity and threshold.  Re-insertion must not
   * trigger another resize until the expanded threshold is reached.
   */
  @Test
  public void testClearPreservesExpandedThreshold() throws Exception {
    java.lang.reflect.Field capField =
        StringHashTableDictionaryV2.class.getDeclaredField("capacity");
    capField.setAccessible(true);
    java.lang.reflect.Field threshField =
        StringHashTableDictionaryV2.class.getDeclaredField("threshold");
    threshField.setAccessible(true);

    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(2);

    // Insert enough keys to trigger several resizes
    for (int i = 0; i < 50; i++) {
      byte[] key = ("pre-clear-" + i).getBytes(StandardCharsets.UTF_8);
      dict.add(key, 0, key.length);
    }
    int expandedCapacity  = capField.getInt(dict);
    int expandedThreshold = threshField.getInt(dict);
    assertTrue(expandedCapacity > 2, "at least one resize must have occurred");

    dict.clear();
    assertEquals(0, dict.size(), "size must be 0 after clear()");

    // clear() must not reset capacity or threshold
    assertEquals(expandedCapacity, capField.getInt(dict),
        "capacity must be preserved after clear()");
    assertEquals(expandedThreshold, threshField.getInt(dict),
        "threshold must be preserved after clear()");

    // Re-insertion below the expanded threshold must NOT trigger any resize
    for (int i = 0; i < expandedThreshold - 1; i++) {
      byte[] key = ("post-clear-" + i).getBytes(StandardCharsets.UTF_8);
      dict.add(key, 0, key.length);
      assertEquals(expandedCapacity, capField.getInt(dict),
          "no resize expected for i=" + i + " (< threshold=" + expandedThreshold + ")");
    }
  }

  /**
   * Stress test: inserts 10,000 unique keys, triggering many resizes, then
   * verifies that all keys are accessible at their original positions and that
   * duplicate detection returns stable indices throughout.
   */
  @Test
  public void testStressTestManyUniqueKeys() throws Exception {
    final int N = 10_000;
    StringHashTableDictionaryV2 dict = new StringHashTableDictionaryV2(16);

    Map<String, Integer> keyToIndex = new HashMap<>();
    for (int i = 0; i < N; i++) {
      String key = "stress-" + i;
      byte[] b = key.getBytes(StandardCharsets.UTF_8);
      int idx = dict.add(b, 0, b.length);
      keyToIndex.put(key, idx);
    }

    assertEquals(N, dict.size(), "size() must equal N after N unique inserts");
    assertEquals(N, new HashSet<>(keyToIndex.values()).size(),
        "every key must receive a unique index");

    // Every key is retrievable at its stored index
    Text text = new Text();
    for (Map.Entry<String, Integer> entry : keyToIndex.entrySet()) {
      dict.getText(text, entry.getValue());
      assertEquals(entry.getKey(), text.toString(),
          "getText at index " + entry.getValue() + " must match original key");
    }

    // Duplicate pass: every re-add must return the same index as before
    for (int i = 0; i < N; i++) {
      String key = "stress-" + i;
      byte[] b = key.getBytes(StandardCharsets.UTF_8);
      assertEquals(keyToIndex.get(key).intValue(), dict.add(b, 0, b.length),
          "duplicate add must return original index for " + key);
    }

    // visit() must cover exactly N unique positions in [0, N-1]
    Set<Integer> visited = new HashSet<>();
    dict.visit(ctx -> visited.add(ctx.getOriginalPosition()));
    assertEquals(N, visited.size(), "visit() must cover all N keys");
    assertEquals(0,     Collections.min(visited));
    assertEquals(N - 1, Collections.max(visited));
  }
}
