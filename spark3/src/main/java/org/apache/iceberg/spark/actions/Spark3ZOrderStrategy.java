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


package org.apache.iceberg.spark.actions;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.spark.FileRewriteCoordinator;
import org.apache.iceberg.spark.FileScanTaskSetManager;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.util.ZOrderByteUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.iceberg.distributions.Distribution;
import org.apache.spark.sql.connector.iceberg.distributions.Distributions;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.TimestampType;
import scala.collection.Seq;

// import org.apache.iceberg.util.Tasks;
// import org.apache.spark.sql.connector.distributions.Distribution;
// import org.apache.spark.sql.connector.distributions.Distributions;
// import org.apache.spark.sql.connector.expressions.SortOrder;

public class Spark3ZOrderStrategy extends Spark3SortStrategy {
  private static final String Z_COLUMN = "ICEZVALUE";
  private static final Schema Z_SCHEMA = new Schema(NestedField.required(0, Z_COLUMN, Types.BinaryType.get()));
  private static final org.apache.iceberg.SortOrder Z_SORT_ORDER = org.apache.iceberg.SortOrder.builderFor(Z_SCHEMA)
      .sortBy(Z_COLUMN, SortDirection.ASC, NullOrder.NULLS_LAST)
      .build();

  private final List<String> zOrderColNames;
  private final FileScanTaskSetManager manager = FileScanTaskSetManager.get();
  private final FileRewriteCoordinator rewriteCoordinator = FileRewriteCoordinator.get();

  public Spark3ZOrderStrategy(Table table, SparkSession spark, List<String> zOrderColNames) {
    super(table, spark);
    this.zOrderColNames = zOrderColNames;
  }

  @Override
  protected void validateOptions() {
    // TODO implement Zorder Strategy in API Module
    return;
  }

  @Override
  public Set<DataFile> rewriteFiles(String groupID, List<FileScanTask> filesToRewrite) {
    boolean requiresRepartition = !filesToRewrite.get(0).spec().equals(table().spec());
    org.apache.spark.sql.connector.iceberg.expressions.SortOrder[] ordering;
    Distribution distribution;
    ordering = Spark3Util.convert(sortOrder());
    if (requiresRepartition) {
      distribution = Spark3Util.buildRequiredDistribution(DistributionMode.RANGE, table());
      ordering = Spark3Util.buildRequiredOrdering(distribution, table().schema(), table().spec(), sortOrder());
    } else {
      distribution = Distributions.ordered(ordering);
    }

    manager.stageTasks(table(), groupID, filesToRewrite);

    // Disable Adaptive Query Execution as this may change the output partitioning of our write
    SparkSession cloneSession = spark().cloneSession();
    cloneSession.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), false);

    // Reset Shuffle Partitions for our sort
    long numOutputFiles = numOutputFiles((long) (inputFileSize(filesToRewrite) * sizeEstimateMultiple()));
    long numShufflePartitions = numOutputFiles * shuffleTasksPerFile();
    cloneSession.conf().set(SQLConf.SHUFFLE_PARTITIONS().key(), Math.max(1, numShufflePartitions));

    Dataset<Row> scanDF = cloneSession.read().format("iceberg")
        .option(SparkReadOptions.FILE_SCAN_TASK_SET_ID, groupID)
        .load(table().name());

    Column[] originalColumns = Arrays.stream(scanDF.schema().names())
        .map(n -> functions.col(n))
        .toArray(Column[]::new);

    List<StructField> zOrderColumns = zOrderColNames.stream()
        .map(scanDF.schema()::apply)
        .collect(Collectors.toList());

    Column zvalueArray = functions.array(zOrderColumns.stream().map(colStruct ->
        SparkZOrder.sortedLexigorpahically(functions.col(colStruct.name()), colStruct.dataType())
    ).toArray(Column[]::new));

    Dataset<Row> zvalueDF = scanDF.withColumn(Z_COLUMN, SparkZOrder.interleaveBytes(zvalueArray));

    // write the packed data into new files where each split becomes a new file
    try {
      SQLConf sqlConf = cloneSession.sessionState().conf();
      LogicalPlan sortPlan = sortPlan(distribution, ordering, numOutputFiles, zvalueDF.logicalPlan(), sqlConf);
      Dataset<Row> sortedDf = new Dataset<>(cloneSession, sortPlan, zvalueDF.encoder());
      sortedDf
          .select(originalColumns)
          .write()
          .format("iceberg")
          .option(SparkWriteOptions.REWRITTEN_FILE_SCAN_TASK_SET_ID, groupID)
          .option(SparkWriteOptions.DISTRIBUTION_MODE, "none")
          .option(SparkWriteOptions.IGNORE_SORT_ORDER, "true")
          .option(RewriteDataFiles.TARGET_FILE_SIZE_BYTES, writeMaxFileSize())
          .mode("append")
          .save(table().name());
    } catch (Exception e) {
      try {
        rewriteCoordinator.abortRewrite(table(), groupID);
        manager.removeTasks(table(), groupID);
      } finally {
        throw e;
      }
    }

    // Actual commit is performed with the groupID
    return rewriteCoordinator.fetchNewDataFiles(table(), ImmutableSet.of(groupID));
  }

  @Override
  protected org.apache.iceberg.SortOrder sortOrder() {
    return Z_SORT_ORDER;
  }

  static class SparkZOrder {

    private static final UserDefinedFunction INT_LIKE_UDF =
        functions.udf((byte[] binary) -> ZOrderByteUtils.orderIntLikeBytes(binary), DataTypes.BinaryType)
            .withName("INT-LEX");
    private static final UserDefinedFunction FLOAT_LIKE_UDF =
        functions.udf((byte[] binary) -> ZOrderByteUtils.orderFloatLikeBytes(binary), DataTypes.BinaryType)
            .withName("FLOAT-LEX");
    private static final UserDefinedFunction UTF_LIKE_UDF =
        functions.udf((byte[] binary) -> ZOrderByteUtils.orderUTF8LikeBytes(binary), DataTypes.BinaryType)
            .withName("STRING-LEX");
    private static final UserDefinedFunction INTERLEAVE_UDF =
        functions.udf((Seq<byte[]> arrayBinary) -> interleaveBits(arrayBinary), DataTypes.BinaryType)
            .withName("INTERLEAVE_BYTES");

    static byte[] interleaveBits(Seq<byte[]> scalaBinary) {
      byte[][] columnsBinary = scala.collection.JavaConverters.seqAsJavaList(scalaBinary)
          .toArray(new byte[scalaBinary.size()][]);
      return ZOrderByteUtils.interleaveBits(columnsBinary);
    }

    static Column interleaveBytes(Column arrayBinary) {
      return INTERLEAVE_UDF.apply(arrayBinary);
    }

    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    static Column sortedLexigorpahically(Column column, DataType type) {
      if (type instanceof ByteType) {
        return column.cast(DataTypes.BinaryType);
      } else if (type instanceof ShortType) {
        return INT_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof IntegerType) {
        return INT_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof LongType) {
        return INT_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof FloatType) {
        return FLOAT_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof DoubleType) {
        return FLOAT_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof DecimalType) {
        // TODO get a better binary representation transformation for decimals
        return UTF_LIKE_UDF.apply(column.cast(DataTypes.StringType).cast(DataTypes.BinaryType));
      } else if (type instanceof StringType) {
        return UTF_LIKE_UDF.apply(column.cast(DataTypes.BinaryType));
      } else if (type instanceof BinaryType) {
        return column;
      } else if (type instanceof BooleanType) {
        return column.cast(DataTypes.BinaryType);
      } else if (type instanceof TimestampType) {
        return INT_LIKE_UDF.apply(column.cast(DataTypes.LongType).cast(DataTypes.BinaryType));
      } else if (type instanceof DateType) {
        return INT_LIKE_UDF.apply(column.cast(DataTypes.LongType).cast(DataTypes.BinaryType));
      } else {
        throw new IllegalArgumentException(
            String.format("Cannot use column %s of type %s in ZOrdering, the type is unsupported",
                column, type));
      }
    }
  }
}
