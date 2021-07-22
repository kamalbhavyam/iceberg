/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestZOrderFilterFiles {
  public final int formatVersion;
  private final Schema schema = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.IntegerType.get()),
      required(3, "xval", Types.IntegerType.get()));
  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  public TestZOrderFilterFiles(int formatVersion) {
    this.formatVersion = formatVersion;
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[]{1, 2};
  }

  @Before
  public void setupTableDir() throws IOException {
    this.tableDir = temp.newFolder();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void testFilterFilesUnpartitionedTableSingleColumn() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFilesSingleColumn(table);
  }

  @Test
  public void testFilterFilesUnpartitionedTableMultipleColumns() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFilesMultipleColumns(table);
  }

  @Test
  public void testFilterFilesDiagramCheck() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = TestTables.create(tableDir, "test", schema, spec, formatVersion);
    testFilterFilesDiagram(table);
  }

  private void testFilterFilesSingleColumn(Table table) {
    Map<Integer, ByteBuffer> lowerBounds1 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds1 = new HashMap<>();
    lowerBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    lowerBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    lowerBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    upperBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    upperBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    Integer zorderLowerBound1 = 1;
    Integer zorderUpperBound1 = 3;
    List<Integer> zorderColumns = new ArrayList<>();
    zorderColumns.add(1);

    Map<Integer, ByteBuffer> lowerBounds2 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds2 = new HashMap<>();
    lowerBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    lowerBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    lowerBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    upperBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    upperBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    upperBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    Integer zorderLowerBound2 = 6;
    Integer zorderUpperBound2 = 8;

    Metrics metrics1 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds1, upperBounds1, zorderLowerBound1, zorderUpperBound1, zorderColumns);

    Metrics metrics2 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds2, upperBounds2, zorderLowerBound2, zorderUpperBound2, zorderColumns);

    DataFile file1 = DataFiles.builder(table.spec())
        .withPath("/path/to/file1.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics1)
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withPath("/path/to/file2.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics2)
        .build();

    table.newAppend().appendFile(file1).commit();
    table.newAppend().appendFile(file2).commit();

    table.refresh();

    TableScan nonEmptyScan = table.newScan().filter(Expressions.equal("data", 4));
    assertEquals(2, Iterables.size(nonEmptyScan.planFiles()));

    TableScan emptyScan = table.newScan().filter(Expressions.equal("id", 4));
    assertEquals(0, Iterables.size(emptyScan.planFiles()));

    TableScan mixScan = table.newScan().filter(Expressions.and(Expressions.equal("id", 8),
        Expressions.equal("data", 6)));
    assertEquals(1, Iterables.size(mixScan.planFiles()));

  }

  private void testFilterFilesMultipleColumns(Table table) {
    Map<Integer, ByteBuffer> lowerBounds1 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds1 = new HashMap<>();
    lowerBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    lowerBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    lowerBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    upperBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    upperBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    upperBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    Integer zorderLowerBound1 = 21;
    Integer zorderUpperBound1 = 38;
    List<Integer> zorderColumns = new ArrayList<>();
    zorderColumns.add(1);
    zorderColumns.add(2);

    Map<Integer, ByteBuffer> lowerBounds2 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds2 = new HashMap<>();
    lowerBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    lowerBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    lowerBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
    upperBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    upperBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    upperBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
    Integer zorderLowerBound2 = 26;
    Integer zorderUpperBound2 = 37;

    Metrics metrics1 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds1, upperBounds1, zorderLowerBound1, zorderUpperBound1, zorderColumns);

    Metrics metrics2 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds2, upperBounds2, zorderLowerBound2, zorderUpperBound2, zorderColumns);

    DataFile file1 = DataFiles.builder(table.spec())
        .withPath("/path/to/file1.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics1)
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withPath("/path/to/file2.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics2)
        .build();

    table.newAppend().appendFile(file1).commit();
    table.newAppend().appendFile(file2).commit();

    table.refresh();

    TableScan nonEmptyScan = table.newScan().filter(Expressions.equal("xval", 6));
    assertEquals(1, Iterables.size(nonEmptyScan.planFiles()));

    TableScan emptyScan = table.newScan().filter(Expressions.equal("id", 4));
    assertEquals(2, Iterables.size(emptyScan.planFiles()));

    TableScan newScan = table.newScan().filter(Expressions.and(Expressions.and(Expressions.equal("id", 3),
        Expressions.equal("data", 6)),
        Expressions.equal("xval", 7)));
    assertEquals(1, Iterables.size(newScan.planFiles()));
  }

  private void testFilterFilesDiagram(Table table) {
    Map<Integer, ByteBuffer> lowerBounds1 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds1 = new HashMap<>();
    lowerBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 0));
    lowerBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 0));
    lowerBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 0));
    upperBounds1.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 3));
    upperBounds1.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 3));
    upperBounds1.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 3));
    Integer zorderLowerBound1 = 0;
    Integer zorderUpperBound1 = 11;
    List<Integer> zorderColumns = new ArrayList<>();
    zorderColumns.add(1);
    zorderColumns.add(2);

    Map<Integer, ByteBuffer> lowerBounds2 = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds2 = new HashMap<>();
    lowerBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 0));
    lowerBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
    lowerBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
    upperBounds2.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
    upperBounds2.put(2, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    upperBounds2.put(3, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
    Integer zorderLowerBound2 = 12;
    Integer zorderUpperBound2 = 19;

    Metrics metrics1 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds1, upperBounds1, zorderLowerBound1, zorderUpperBound1, zorderColumns);

    Metrics metrics2 = new Metrics(2L, Maps.newHashMap(), Maps.newHashMap(),
        Maps.newHashMap(), lowerBounds2, upperBounds2, zorderLowerBound2, zorderUpperBound2, zorderColumns);

    DataFile file1 = DataFiles.builder(table.spec())
        .withPath("/path/to/file1.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics1)
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
        .withPath("/path/to/file2.parquet")
        .withFileSizeInBytes(0)
        .withMetrics(metrics2)
        .build();

    table.newAppend().appendFile(file1).commit();
    table.newAppend().appendFile(file2).commit();

    table.refresh();

    TableScan diagramScan = table.newScan().filter(Expressions.and(Expressions.equal("id", 0),
        Expressions.equal("data", 3)));
    assertEquals(1, Iterables.size(diagramScan.planFiles()));
  }
}
// (id==4 && data==6) || (id==3 && xval==3)
