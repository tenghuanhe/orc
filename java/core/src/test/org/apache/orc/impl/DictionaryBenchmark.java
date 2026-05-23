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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * JMH throughput benchmarks for {@link Dictionary} implementations used in ORC string
 * column encoding.
 *
 * <p><b>Run in IntelliJ:</b> right-click {@link #run()} and select "Run".
 * <br><b>Run via Maven:</b> {@code mvn test -pl core -Dtest=DictionaryBenchmark#run}
 * <br><b>Skip in CI:</b> configure surefire with
 * {@code <excludedGroups>benchmark</excludedGroups>}.
 *
 * <p><b>Adding a new implementation variant:</b>
 * <ol>
 *   <li>Add a {@code case} in {@link #createDictionary(String)} that returns your impl.</li>
 *   <li>Add the name to the {@code @Param} list on {@link #impl}.</li>
 * </ol>
 */
@Tag("benchmark")
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 0)   // in-process: no subprocess needed, works directly in IntelliJ/surefire
@State(Scope.Thread)
public class DictionaryBenchmark {

  // ---- Parameters ----------------------------------------------------------

  /**
   * Dictionary implementation name. Add new variant names here, then add a
   * corresponding {@code case} in {@link #createDictionary(String)}.
   */
  @Param({"V1", "V2"})
  public String impl;

  /**
   * Number of distinct strings per stripe. Small (1 000) and medium (50 000)
   * cover the range where ORC typically enables dictionary encoding.
   */
  @Param({"1000", "50000"})
  public int distinctKeys;

  // ---- State ---------------------------------------------------------------

  private byte[][] keyBytes;  // pre-encoded UTF-8 key bytes, one entry per distinct key
  private int[] keyLens;
  private Dictionary dict;

  @Setup(Level.Trial)
  public void setUp() {
    keyBytes = new byte[distinctKeys][];
    keyLens = new int[distinctKeys];
    for (int i = 0; i < distinctKeys; i++) {
      byte[] b = String.format("key-%09d", i).getBytes(StandardCharsets.UTF_8);
      keyBytes[i] = b;
      keyLens[i] = b.length;
    }
    dict = createDictionary(impl);
  }

  /** Pre-populates the dictionary before each measurement iteration. */
  @Setup(Level.Iteration)
  public void populateDict() {
    dict.clear();
    for (int i = 0; i < distinctKeys; i++) {
      dict.add(keyBytes[i], 0, keyLens[i]);
    }
  }

  // ---- Benchmarks ----------------------------------------------------------

  /**
   * Simulates encoding one full ORC stripe: clears the dictionary then inserts
   * all {@link #distinctKeys} strings once. Measures combined clear + bulk-insert
   * throughput in stripes/ms.
   *
   * <p>This is the write-path hot loop: every row in the stripe calls
   * {@code dictionary.add()} with the column value.
   */
  @Benchmark
  public int encodeStripe() {
    dict.clear();
    int last = 0;
    for (int i = 0; i < distinctKeys; i++) {
      last = dict.add(keyBytes[i], 0, keyLens[i]);
    }
    return last;   // returned to JMH to prevent dead-code elimination
  }

  /**
   * Measures lookup throughput when all keys are already present.
   * The dictionary is pre-populated by {@link #populateDict()}, so every
   * {@code add()} call here returns an existing index without modifying the table.
   *
   * <p>This models high-cardinality-but-steady-state workloads where the
   * dictionary fills up in the first few rows of a stripe and the rest are
   * pure lookups.
   */
  @Benchmark
  public int lookupAll() {
    int last = 0;
    for (int i = 0; i < distinctKeys; i++) {
      last = dict.add(keyBytes[i], 0, keyLens[i]);
    }
    return last;
  }

  // ---- JUnit entry point ---------------------------------------------------

  /**
   * Runs all benchmarks in-process and prints results to stdout.
   * <b>Right-click this method in IntelliJ and select "Run"</b>, or invoke via:
   * <pre>mvn test -pl core -Dtest=DictionaryBenchmark#run</pre>
   */
  @Test
  public void run() throws Exception {
    Options opts = new OptionsBuilder()
        .include(getClass().getSimpleName())
        .forks(0)
        .warmupIterations(1)
        .warmupTime(TimeValue.seconds(1))
        .measurementIterations(3)
        .measurementTime(TimeValue.seconds(1))
        .build();
    new Runner(opts).run();
  }

  // ---- Factory -------------------------------------------------------------

  /**
   * Creates a {@link Dictionary} for the given implementation name.
   *
   * <p>To add a new variant, add a {@code case} here and add the name to the
   * {@code @Param} annotation on {@link #impl}. The initial capacity is
   * {@link Dictionary#INITIAL_DICTIONARY_SIZE} to match production defaults.
   */
  public static Dictionary createDictionary(String impl) {
    return switch (impl) {
      case "V1" -> new StringHashTableDictionary(Dictionary.INITIAL_DICTIONARY_SIZE);
      case "V2" -> new StringHashTableDictionaryV2(Dictionary.INITIAL_DICTIONARY_SIZE);
      default -> throw new IllegalArgumentException("Unknown impl: " + impl);
    };
  }
}
