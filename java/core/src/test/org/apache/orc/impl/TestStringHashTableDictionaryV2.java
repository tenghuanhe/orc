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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
