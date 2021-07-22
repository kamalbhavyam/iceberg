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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestManifestReaderWithZOrder extends TableTestBase {
  private static final Map<Integer, Long> VALUE_COUNT = ImmutableMap.of(3, 3L);
  private static final Map<Integer, Long> NULL_VALUE_COUNTS = ImmutableMap.of(3, 0L);
  private static final Map<Integer, Long> NAN_VALUE_COUNTS = ImmutableMap.of(3, 1L);
  private static final List<Integer> ZORDER_COLUMNS = ImmutableList.of(3);
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS1 =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS1 =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 5));
  private static final Integer ZORDER_LOWER_BOUND1 = 1;
  private static final Integer ZORDER_UPPER_BOUND1 = 3;
  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS2 =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 4));
  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS2 =
      ImmutableMap.of(3, Conversions.toByteBuffer(Types.IntegerType.get(), 7));
  private static final Integer ZORDER_LOWER_BOUND2 = 6;
  private static final Integer ZORDER_UPPER_BOUND2 = 7;
  private static final DataFile FILE1 = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(3)
      .withMetrics(new Metrics(3L, null,
          VALUE_COUNT, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS,
          LOWER_BOUNDS1, UPPER_BOUNDS1, ZORDER_LOWER_BOUND1, ZORDER_UPPER_BOUND1, ZORDER_COLUMNS))
      .build();
  private static final DataFile FILE2 = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("data_bucket=0") // easy way to set partition data for now
      .withRecordCount(3)
      .withMetrics(new Metrics(3L, null,
          VALUE_COUNT, NULL_VALUE_COUNTS, NAN_VALUE_COUNTS,
          LOWER_BOUNDS2, UPPER_BOUNDS2, ZORDER_LOWER_BOUND2, ZORDER_UPPER_BOUND2, ZORDER_COLUMNS))
      .build();

  public TestManifestReaderWithZOrder(int formatVersion) {
    super(formatVersion);
  }

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[]{1, 2};
  }

  @Test
  public void testReadIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE1, FILE2);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      CloseableIterator<ManifestEntry<DataFile>> entryitr = entries.iterator();
      ManifestEntry<DataFile> entry = entryitr.next();
      assertStatsFile1(entry.file());
      entry = entryitr.next();
      assertStatsFile2(entry.file());
    }
  }

  @Test
  public void testReadEntriesWithFilterAndSelectIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE1, FILE2);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableList.of("file_path"))
        .filterRows(Expressions.equal("id", 3))) {
      CloseableIterable<ManifestEntry<DataFile>> entries = reader.entries();
      CloseableIterator<ManifestEntry<DataFile>> entryitr = entries.iterator();
      ManifestEntry<DataFile> entry = entryitr.next();
      assertStatsFile1(entry.file());
      entry = entryitr.next();
      assertStatsFile2(entry.file());
    }
  }

  @Test
  public void testReadIteratorWithFilterAndSelectRecordCountDropsStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE1, FILE2);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableList.of("file_path", "record_count"))
        .filterRows(Expressions.equal("id", 3))) {
//            DataFile entry = reader.iterator().next();
      CloseableIterator<DataFile> entryitr = reader.iterator();
      DataFile entry = entryitr.next();
      assertStatsFile1Dropped(entry);
      entry = entryitr.next();
      assertStatsFile2Dropped(entry);
    }
  }

  @Test
  public void testReadIteratorWithFilterAndSelectStatsIncludesFullStats() throws IOException {
    ManifestFile manifest = writeManifest(1000L, FILE1, FILE2);
    try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
        .select(ImmutableList.of("file_path", "value_counts"))
        .filterRows(Expressions.equal("id", 3))) {
//            DataFile entry = reader.iterator().next();
//            assertFullStats(entry);
      CloseableIterator<DataFile> entryitr = reader.iterator();
      DataFile entry = entryitr.next();
      assertStatsFile1(entry);
      assertStatsFile1Dropped(entry.copyWithoutStats());
      entry = entryitr.next();
      assertStatsFile2(entry);
      assertStatsFile2Dropped(entry.copyWithoutStats());

      // explicitly call copyWithoutStats and ensure record count will not be dropped
//            assertStatsDropped(entry.copyWithoutStats());
    }
  }

  private void assertStatsFile1(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount());
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertEquals(VALUE_COUNT, dataFile.valueCounts());
    Assert.assertEquals(NULL_VALUE_COUNTS, dataFile.nullValueCounts());
    Assert.assertEquals(NAN_VALUE_COUNTS, dataFile.nanValueCounts());
    Assert.assertEquals(ZORDER_COLUMNS, dataFile.zorderColumns());
    Assert.assertEquals(LOWER_BOUNDS1, dataFile.lowerBounds());
    Assert.assertEquals(UPPER_BOUNDS1, dataFile.upperBounds());
    Assert.assertEquals(ZORDER_LOWER_BOUND1, dataFile.zorderLowerBound());
    Assert.assertEquals(ZORDER_UPPER_BOUND1, dataFile.zorderUpperBound());

    Assert.assertEquals("/path/to/data-a.parquet", dataFile.path()); // always select file path in all test cases
  }

  private void assertStatsFile2(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount());
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertEquals(VALUE_COUNT, dataFile.valueCounts());
    Assert.assertEquals(NULL_VALUE_COUNTS, dataFile.nullValueCounts());
    Assert.assertEquals(NAN_VALUE_COUNTS, dataFile.nanValueCounts());
    Assert.assertEquals(ZORDER_COLUMNS, dataFile.zorderColumns());
    Assert.assertEquals(LOWER_BOUNDS2, dataFile.lowerBounds());
    Assert.assertEquals(UPPER_BOUNDS2, dataFile.upperBounds());
    Assert.assertEquals(ZORDER_LOWER_BOUND2, dataFile.zorderLowerBound());
    Assert.assertEquals(ZORDER_UPPER_BOUND2, dataFile.zorderUpperBound());

    Assert.assertEquals("/path/to/data-b.parquet", dataFile.path()); // always select file path in all test cases
  }

  private void assertStatsFile1Dropped(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount()); // record count is not considered as droppable stats
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertNull(dataFile.valueCounts());
    Assert.assertNull(dataFile.nullValueCounts());
    Assert.assertNull(dataFile.zorderColumns());
    Assert.assertNull(dataFile.lowerBounds());
    Assert.assertNull(dataFile.upperBounds());
    Assert.assertNull(dataFile.nanValueCounts());

    Assert.assertEquals("/path/to/data-a.parquet", dataFile.path()); // always select file path in all test cases
  }

  private void assertStatsFile2Dropped(DataFile dataFile) {
    Assert.assertEquals(3, dataFile.recordCount()); // record count is not considered as droppable stats
    Assert.assertNull(dataFile.columnSizes());
    Assert.assertNull(dataFile.valueCounts());
    Assert.assertNull(dataFile.nullValueCounts());
    Assert.assertNull(dataFile.zorderColumns());
    Assert.assertNull(dataFile.lowerBounds());
    Assert.assertNull(dataFile.upperBounds());
    Assert.assertNull(dataFile.nanValueCounts());

    Assert.assertEquals("/path/to/data-b.parquet", dataFile.path()); // always select file path in all test cases
  }
}
