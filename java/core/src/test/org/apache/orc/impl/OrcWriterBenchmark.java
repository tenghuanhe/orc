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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * End-to-end JMH benchmarks for the ORC writer with different dictionary implementations.
 * Measures the time to encode and write one full stripe ({@value #ROWS_PER_STRIPE} rows,
 * one string column, no compression) to a local temp file.
 *
 * <p>What is measured: dictionary lookup/insert, integer RLE encoding of row indices,
 * ORC footer serialization, and local-disk write. Compression is disabled to isolate
 * dictionary-encoding throughput from codec overhead.
 *
 * <p><b>Important:</b> {@code DICTIONARY_KEY_SIZE_THRESHOLD} is forced to {@code 1.0}
 * and {@code ROW_INDEX_STRIDE_DICTIONARY_CHECK} is disabled so the writer always uses
 * dictionary encoding regardless of cardinality. Without this, ORC's built-in heuristic
 * disables dictionary encoding after the first row-index stride (10K rows) when
 * {@code distinctValues / strideRows > 0.8}, making all implementations measure the
 * same direct-encoding path instead of the dictionary.
 *
 * <p><b>Run in IntelliJ:</b> right-click {@link #run()} and select "Run".
 * <br><b>Run via Maven:</b> {@code mvn test -pl core -Dtest=OrcWriterBenchmark#run}
 * <br><b>Skip in CI:</b> add surefire {@code <excludedGroups>benchmark</excludedGroups>}.
 *
 * <p><b>Adding a new variant:</b> add the impl name to the {@code @Param} list on
 * {@link #dictImpl} — it is passed directly to {@link OrcConf#DICTIONARY_IMPL}.
 */
@Tag("benchmark")
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(value = 0)   // in-process: no subprocess needed, works directly in IntelliJ/surefire
@State(Scope.Thread)
public class OrcWriterBenchmark {

  // ---- Parameters ----------------------------------------------------------

  /**
   * Dictionary implementation name passed to {@link OrcConf#DICTIONARY_IMPL}.
   * Valid values: {@code HASH}, {@code HASH_V2}, {@code RBTREE}.
   */
  @Param({"HASH", "HASH_V2", "RBTREE"})
  public String dictImpl;

  /**
   * Number of distinct string values in the stripe.
   * Low cardinality (1K) exercises repeated lookup; high (50K) exercises
   * insert-heavy paths and resize.
   *
   * <p>Note: dictionary encoding stays active at all cardinalities because
   * {@code DICTIONARY_KEY_SIZE_THRESHOLD=1.0} is set in {@link #setUp()}.
   */
  @Param({"1000", "50000"})
  public int distinctValues;

  // ---- Constants -----------------------------------------------------------

  /** Rows written per benchmark invocation (= one stripe). */
  static final int ROWS_PER_STRIPE = 100_000;

  // ---- State ---------------------------------------------------------------

  private byte[][] valuePool;   // pre-encoded UTF-8 byte arrays, one per distinct value
  private int[] valueLens;
  private OrcFile.WriterOptions writerOptions;
  private TypeDescription schema;
  private Path outputPath;
  private File outputFile;

  @Setup(Level.Trial)
  public void setUp() throws Exception {
    // Pre-generate the value pool so UTF-8 encoding is not charged to the benchmark.
    valuePool = new byte[distinctValues][];
    valueLens = new int[distinctValues];
    for (int i = 0; i < distinctValues; i++) {
      byte[] b = String.format("val-%09d", i).getBytes(StandardCharsets.UTF_8);
      valuePool[i] = b;
      valueLens[i] = b.length;
    }

    // Temp file reused (overwritten) across invocations.
    outputFile = File.createTempFile("orc-e2e-bench", ".orc");
    outputPath = new Path(outputFile.getAbsolutePath());

    // Build writer options once; createWriter() reads but does not mutate them.
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    conf.set("fs.file.impl.disable.cache", "true");
    conf.set(OrcConf.OVERWRITE_OUTPUT_FILE.getAttribute(), "true");
    conf.set(OrcConf.DICTIONARY_IMPL.getAttribute(), dictImpl);
    // Force dictionary encoding regardless of cardinality.
    // Default threshold (0.8) would disable dictionary after the first row-index stride
    // (10K rows) whenever distinctValues/strideRows > 0.8, causing all impls to fall
    // through to identical direct-encoding code and making the benchmark useless.
    conf.set(OrcConf.DICTIONARY_KEY_SIZE_THRESHOLD.getAttribute(), "1.0");
    // Disable the per-stride early-exit check so the threshold above is the sole gate.
    conf.set(OrcConf.ROW_INDEX_STRIDE_DICTIONARY_CHECK.getAttribute(), "false");

    schema = TypeDescription.fromString("struct<value:string>");
    writerOptions = OrcFile.writerOptions(conf)
        .setSchema(schema)
        .compress(CompressionKind.NONE);   // isolate encoding from codec overhead
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    if (outputFile != null) {
      outputFile.delete();
    }
  }

  // ---- Benchmark -----------------------------------------------------------

  /**
   * Writes one full ORC stripe ({@value #ROWS_PER_STRIPE} rows, one string column)
   * to a local temp file, then closes the writer (which flushes the stripe and footer).
   *
   * <p>The value at each row index is {@code valuePool[row % distinctValues]}, giving
   * a uniform round-robin distribution over the dictionary. The returned row count
   * prevents dead-code elimination.
   */
  @Benchmark
  public long writeStripe() throws Exception {
    long rowsWritten = 0;
    try (Writer writer = OrcFile.createWriter(outputPath, writerOptions)) {
      VectorizedRowBatch batch = schema.createRowBatch();
      BytesColumnVector col = (BytesColumnVector) batch.cols[0];

      for (int row = 0; row < ROWS_PER_STRIPE; row++) {
        int valIdx = row % distinctValues;
        col.setVal(batch.size, valuePool[valIdx], 0, valueLens[valIdx]);
        batch.size++;
        if (batch.size == batch.getMaxSize()) {
          writer.addRowBatch(batch);
          rowsWritten += batch.size;
          batch.reset();
        }
      }
      if (batch.size > 0) {
        writer.addRowBatch(batch);
        rowsWritten += batch.size;
      }
    }
    return rowsWritten;
  }

  // ---- JUnit entry point ---------------------------------------------------

  /**
   * Runs all benchmarks in-process and prints results to stdout.
   * <b>Right-click this method in IntelliJ and select "Run"</b>, or invoke via:
   * <pre>mvn test -pl core -Dtest=OrcWriterBenchmark#run</pre>
   */
  @Test
  public void run() throws Exception {
    Options opts = new OptionsBuilder()
        .include(getClass().getSimpleName())
        .forks(0)
        .warmupIterations(1)
        .warmupTime(TimeValue.seconds(2))
        .measurementIterations(3)
        .measurementTime(TimeValue.seconds(2))
        .build();
    new Runner(opts).run();
  }
}
