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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Term;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.NullOrder.NULLS_LAST;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestZOrdering {
  private static final Schema SCHEMA = new Schema(
      required(10, "id", Types.IntegerType.get()),
      required(11, "data", Types.IntegerType.get()),
      required(40, "d", Types.DateType.get()),
      required(41, "ts", Types.TimestampType.withZone()),
      optional(12, "s", Types.StructType.of(
          required(17, "id", Types.IntegerType.get()),
          optional(18, "b", Types.ListType.ofOptional(3, Types.StructType.of(
              optional(19, "i", Types.IntegerType.get()),
              optional(20, "s", Types.StringType.get())
          )))
      )),
      required(30, "ext", Types.StringType.get()));

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  private final int formatVersion;

  public TestZOrdering(int formatVersion) {
    this.formatVersion = formatVersion;
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
  public void testZorder() {
    SortOrder id = SortOrder.builderFor(SCHEMA)
        .asc(Expressions.zorder("id", "data")
            .stream()
            .map(m -> (Term) m)
            .collect(Collectors.toList()), NULLS_LAST)
        .build();
    PartitionSpec spec = PartitionSpec.unpartitioned();
    TestTables.TestTable table = TestTables.create(tableDir, "test", SCHEMA, spec, id, formatVersion);

    SortOrder actualOrder = table.sortOrder();

    Transform<List<Object>, Integer> zvals = Transforms.zorder();

    List<Object> testmap = new ArrayList<>();
    testmap.add(0);
    testmap.add(1);
    testmap.add(2);

    Assert.assertEquals("eyo wut", 0, (int) zvals.apply(testmap));
  }
}
