/*
 * Copyright © 2020 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin;

import com.google.common.base.Strings;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.plugin.function.DiscretePercentile;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

/**
 * Util method for {@link WindowAggregation}.
 *
 * This class contains methods for {@link WindowAggregation} that require spark classes because during validation
 * spark classes are not available. Refer CDAP-15912 for more information.
 */
final class WindowsAggregationUtil {

  private WindowsAggregationUtil() {
    //no op
  }

  public static JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                                    JavaRDD<StructuredRecord> javaRDD, WindowAggregationConfig config,
                                                    Schema inputSchema, Schema outputSchema) {
    JavaRDD<Row> map = javaRDD.map(
      structuredRecord -> DataFrames.toRow(structuredRecord, DataFrames.toDataType(inputSchema)));

    SQLContext sqlContext = new SQLContext(sparkExecutionPluginContext.getSparkContext());

    Dataset<Row> data = sqlContext.createDataFrame(map, DataFrames.toDataType(inputSchema));
    WindowSpec spec = Window.partitionBy(WindowsAggregationUtil.getPartitionsColumns(config.getPartitionFields()))
      .orderBy(WindowsAggregationUtil.getPartitionOrderColumns(config.getPartitionOrder()));

    switch (config.getWindowFrameType()) {
      case ROW:
        spec = spec.rowsBetween(config.getFrameDefinitionPrecedingBound(),
                                config.getFrameDefinitionFollowingBound());
        break;
      case RANGE:
        spec = spec.rangeBetween(config.getFrameDefinitionPrecedingBound(),
                                 config.getFrameDefinitionFollowingBound());
        break;
      case NONE:
      default:
    }

    List<WindowAggregationConfig.FunctionInfo> aggregatesData =
      config.getAggregates(sparkExecutionPluginContext.getFailureCollector());

    for (WindowAggregationConfig.FunctionInfo aggregatesDatum : aggregatesData) {
      if (aggregatesDatum.getFunction() == WindowAggregationConfig.Function.DISCRETE_PERCENTILE) {
        data = applyCustomFunction(sqlContext, aggregatesDatum, data, spec, inputSchema);
      } else {
        data = apply(aggregatesDatum, data, spec);
      }
    }

    JavaRDD<StructuredRecord> rowJavaRDD = data.javaRDD().map(new RowToRecord(outputSchema));

    String numberOfPartitionsString = config.getNumberOfPartitions();

    if (Strings.isNullOrEmpty(numberOfPartitionsString)) {
      return rowJavaRDD;
    }

    return rowJavaRDD.repartition(Integer.parseInt(numberOfPartitionsString));
  }

  private static Dataset<Row> applyCustomFunction(SQLContext sqlContext, WindowAggregationConfig.FunctionInfo
    aggregatesDatum, Dataset<Row> dataFrame, WindowSpec spec, Schema inputSchema) {
    String[] args = aggregatesDatum.getArgs();
    if (args.length < 1) {
      throw new InvalidParameterException("Discrete Percentile must have at least 1 arguments");
    }

    String percentileString = args[0];
    double percentile;
    try {
      percentile = Double.parseDouble(percentileString);
    } catch (NumberFormatException e) {
      throw new InvalidParameterException("Discrete Percentile, first argument must be double value");
    }

    if (percentile > 1 || percentile < 0) {
      throw new InvalidParameterException("Discrete Percentile, percentile must be in range 0.0-1.0");
    }

    Schema discretePercentileInputSchema = schemaForInputField(inputSchema, aggregatesDatum.getFieldName());
    DiscretePercentile discretePercentile = new DiscretePercentile(percentile, discretePercentileInputSchema);
    sqlContext.udf().register("DISCRETE_PERCENTILE", discretePercentile);

    return apply(aggregatesDatum, dataFrame, spec);
  }

  private static Dataset<Row> apply(WindowAggregationConfig.FunctionInfo data, Dataset<Row> dataFrame,
                                    WindowSpec spec) {
    Column aggregateColumn = getAggregateColumn(data).over(spec);
    return dataFrame.withColumn(data.getAlias(), aggregateColumn);
  }

  private static Column getAggregateColumn(WindowAggregationConfig.FunctionInfo data) {
    String[] args = data.getArgs();
    switch (data.getFunction()) {
      case RANK:
        return functions.rank();
      case DENSE_RANK:
        return functions.dense_rank();
      case PERCENT_RANK:
        return functions.percent_rank();
      case N_TILE:
        checkFunctionArguments(args, 1, "N tile");
        return functions.ntile(Integer.parseInt(args[0]));
      case ROW_NUMBER:
        return functions.row_number();
      case MEDIAN:
        return functions.callUDF("percentile", functions.col(data.getFieldName()), functions.lit(0.5));
      case CONTINUOUS_PERCENTILE: {
        checkFunctionArguments(args, 1, "Continuous Percentile");
        String arg = args[0];
        Column lit = functions.lit(arg);
        return functions.callUDF("percentile", functions.col(data.getFieldName()), lit);
      }
      case DISCRETE_PERCENTILE: {
        return functions.callUDF("DISCRETE_PERCENTILE", functions.col(data.getFieldName()));
      }
      case LEAD: {
        checkFunctionArguments(args, 1, "LEAD");
        return functions.lead(data.getFieldName(), Integer.parseInt(args[0]));
      }
      case LAG:
        checkFunctionArguments(args, 1, "LAG");
        return functions.lag(data.getFieldName(), Integer.parseInt(args[0]));
      case FIRST: {
        boolean ignoreNull = data.isIgnoreNull();
        if (args.length != 0) {
          ignoreNull = ignoreNull || Boolean.parseBoolean(args[0]);
        }
        return functions.first(data.getFieldName(), ignoreNull);
      }
      case LAST: {
        boolean ignoreNull = data.isIgnoreNull();
        if (args.length != 0) {
          ignoreNull = ignoreNull || Boolean.parseBoolean(args[0]);
        }
        return functions.last(data.getFieldName(), ignoreNull);
      }
      case CUMULATIVE_DISTRIBUTION:
        return functions.cume_dist();
      case ACCUMULATE:
        return functions.sum(data.getFieldName());
      default:
        throw new InvalidParameterException("Can not find function " + data.getFieldName());
    }
  }

  private static void checkFunctionArguments(String[] args, int minimumArguments, String function) {
    if (args.length < minimumArguments) {
      throw new InvalidParameterException(
        String.format("%s must have at least %s arguments", function, minimumArguments));
    }
  }

  private static Schema schemaForInputField(Schema inputSchema, String inputFieldName) {
    if (inputSchema == null) {
      return null;
    }

    Schema.Field schemaField = inputSchema.getField(inputFieldName);
    if (schemaField == null) {
      return null;
    }

    return schemaField.getSchema();
  }

  private static Column[] getPartitionsColumns(List<String> partitionsFields) {
    return partitionsFields.stream().map(Column::new).toArray(Column[]::new);
  }

  public static Column[] getPartitionOrderColumns(String partitionOrder) {
    if (partitionOrder == null || partitionOrder.isEmpty()) {
      return new Column[0];
    }
    List<Column> columns = new ArrayList<>();
    String[] columnsAndOrder = partitionOrder.split(",");
    for (String columnAndOrder : columnsAndOrder) {
      String[] split = columnAndOrder.split(":");

      String columnName = split[0];
      String orderString = split[1];

      Column column = new Column(columnName);
      WindowAggregationConfig.Order order = WindowAggregationConfig.Order.fromString(orderString);
      column = WindowAggregationConfig.Order.ASCENDING == order ? column.asc() : column.desc();

      columns.add(column);
    }
    return columns.toArray(new Column[columns.size()]);
  }
}
