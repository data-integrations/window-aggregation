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


import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.function.DiscretePercentile;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Window aggregator plugin
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(WindowAggregation.NAME)
@Description("Specify a window over which functions should be applied. \n" +
  "Supports functions: Rank, Dense Rank, Percent Rank, N tile, Row Number, Median," +
  "Continuous Percentile, Lead, Lag, First, Last, Cumulative distribution, Accumulate.")
public class WindowAggregation extends SparkCompute<StructuredRecord, StructuredRecord> {

  public static final String NAME = "WindowAggregation";
  private final WindowAggregationConfig config;
  private Schema outputSchema;

  private static final Logger LOG = LoggerFactory.getLogger(WindowAggregation.class);

  public WindowAggregation(WindowAggregationConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {

    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    List<String> partitionFields = config.getPartitionFields();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates();
    List<String> partitionOrderFields = config.getPartitionOrderFields();

    if (inputSchema == null || partitionFields.isEmpty() || aggregates.isEmpty()) {
      stageConfigurer.setOutputSchema(null);
      return;
    }

    validate(inputSchema, partitionFields, aggregates, partitionOrderFields, stageConfigurer.getFailureCollector());
    stageConfigurer.getFailureCollector().getOrThrowException();
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, aggregates));
  }

  public void validate(Schema inputSchema, List<String> partitionFields,
                       List<WindowAggregationConfig.FunctionInfo> aggregates, List<String> partitionOrderFields,
                       FailureCollector collector) {

    for (String partitionField : partitionFields) {
      Schema.Field field = inputSchema.getField(partitionField);
      if (field == null) {
        collector.addFailure(
          String.format("Can not partition by field '%s' because it does not exist in input schema.", partitionField),
          String.format("Partition field '%s' must exist in input schema.", partitionField))
          .withConfigElement("partitionFields", partitionField);
      }
    }

    for (WindowAggregationConfig.FunctionInfo functionInfo : aggregates) {
      String functionFieldName = functionInfo.getFieldName();
      if (Strings.isNullOrEmpty(functionFieldName)) {
        continue; //function will operate over partitions fields
      }

      Schema.Field inputField = inputSchema.getField(functionFieldName);
      Schema inputFieldSchema = null;
      if (inputField == null) {
        collector.addFailure(
          String.format("Invalid aggregate %s: Field '%s' does not exist in input schema.", functionInfo.description(),
                        functionFieldName), String.format("Field '%s' must exist in input schema.", functionFieldName))
          .withConfigProperty("aggregates");
      } else {
        inputFieldSchema = inputField.getSchema();
      }

      WindowAggregationConfig.Function function = functionInfo.getFunction();
      //check input schema
      Schema allowedInputScheme = function.getAllowedInputScheme();


      if (allowedInputScheme != null && inputFieldSchema != null) {
        Schema.Type type = inputFieldSchema.isNullable() ? inputFieldSchema.getUnionSchema(0).getType() :
          inputFieldSchema.getType();
        int indexOf = allowedInputScheme.getUnionSchemas().stream().map(Schema::getType).collect(Collectors.toList())
          .indexOf(type);
        if (indexOf < 0) {
          collector.addFailure(
            String.format("Invalid input schema type '%s' for field '%s' in function '%s'.",
                          type, inputField.getName(), function.name())
            , String.format("Allowed input types for function '%s' are '%s'.",
                            function.name(), Joiner.on(",").join(allowedInputScheme.getUnionSchemas())));
        }
      }

      if (function.getOutputSchema() == null && inputFieldSchema != null) {
        function.setOutputSchema(inputFieldSchema);
      }
    }

    for (String partitionOrderField : partitionOrderFields) {
      Schema.Field field = inputSchema.getField(partitionOrderField);
      if (field == null) {
        collector.addFailure(
          String.format("Can not order by field '%s' because it does not exist in input schema", partitionOrderField),
          "Order field '%s' should exist must exist in input schema.").
          withConfigProperty("partitionFieldsOrder");
      }
    }
  }

  public Schema getOutputSchema(Schema inputSchema, List<WindowAggregationConfig.FunctionInfo> aggregates) {
    List<Schema.Field> outputFields = new ArrayList<>(aggregates.size());
    for (WindowAggregationConfig.FunctionInfo aggregate : aggregates) {
      outputFields.add(Schema.Field.of(aggregate.getAlias(), aggregate.getFunction().getOutputSchema()));
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".window", outputFields);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    super.prepareRun(context);
    validate(context.getInputSchema(), config.getPartitionFields(), config.getAggregates(),
             config.getPartitionOrderFields(), context.getFailureCollector());
    Schema outputSchema = getOutputSchema(context.getInputSchema(), config.getAggregates());
    recordLineage(context, outputSchema);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    validate(context.getInputSchema(), config.getPartitionFields(), config.getAggregates(),
             config.getPartitionOrderFields(), context.getFailureCollector());
    outputSchema = getOutputSchema(context.getInputSchema(), config.getAggregates());
  }

  private void recordLineage(SparkPluginContext context, Schema schema) {
    if (schema == null) {
      LOG.debug("The output schema is null. Field level lineage will not be recorded");
      return;
    }
    if (schema.getFields() == null) {
      LOG.debug("The output schema fields are null. Field level lineage will not be recorded");
      return;
    }

    List<String> partitionFields = config.getPartitionFields();
    String columnsPostFix = partitionFields.size() > 1 ? "s" : "";
    String partitionFieldsDescription = Joiner.on(",").join(partitionFields);

    String orderDescription = generateOrderDescription();
    String frameDescription = generateFrameDescription(config);

    LinkedList<FieldOperation> fllOperations = new LinkedList<>();
    // for every function record the field level operation details
    for (WindowAggregationConfig.FunctionInfo functionInfo : config.getAggregates()) {

      String functionDescription = generateFunctionDescription(functionInfo);
      String description = String.format("Generated field '%s' by partitioning on the column%s '%s' %s %s %s",
                                         functionInfo.getAlias(), columnsPostFix, partitionFieldsDescription,
                                         orderDescription, functionDescription, frameDescription);
      String operationName = String.format("Window function over %s", functionInfo.getFieldName());
      FieldOperation operation = new FieldTransformOperation(operationName, description,
                                                             Collections.singletonList(functionInfo.getFieldName()),
                                                             functionInfo.getAlias());
      fllOperations.add(operation);
    }
    context.record(fllOperations);
  }

  private String generateFunctionDescription(WindowAggregationConfig.FunctionInfo functionInfo) {
    String fieldName = functionInfo.getFieldName();
    String extension = Strings.isNullOrEmpty(fieldName) ? "" : " on field : " + fieldName + "";
    return String.format(", and then applying the function %s %s", functionInfo.getFunction().name(), extension);
  }

  private String generateOrderDescription() {
    String partitionOrder = config.getPartitionOrder();
    if (Strings.isNullOrEmpty(partitionOrder)) {
      return ", without sorting results";
    }
    return String.format(", sorting results by fields: %s", partitionOrder);
  }

  private String generateFrameDescription(WindowAggregationConfig config) {
    boolean isUnboundedPreceding = config.isFrameDefinitionUnboundedPreceding();
    boolean isUnboundedFollowing = config.isFrameDefinitionUnboundedFollowing();

    WindowAggregationConfig.WindowFrameType windowFrameType = config.getWindowFrameType();
    switch (windowFrameType) {
      case NONE:
        return ".";
      case ROW:
        return String.format("over a windows frame consisting of %s preceding and %s following rows.",
                             isUnboundedPreceding ? "unbound" : config.getFrameDefinitionPrecedingBound(),
                             isUnboundedFollowing ? "unbound" : config.getFrameDefinitionFollowingBound());
      case RANGE:
        return String.format("over a windows frame consisting of %s preceding and %s following range.",
                             isUnboundedPreceding ? "unbound" : config.getFrameDefinitionPrecedingBound(),
                             isUnboundedFollowing ? "unbound" : config.getFrameDefinitionFollowingBound());
    }

    return "";
  }

  @Override
  public JavaRDD<StructuredRecord> transform(
    SparkExecutionPluginContext sparkExecutionPluginContext, JavaRDD<StructuredRecord> javaRDD) throws Exception {
    Schema inputSchema = sparkExecutionPluginContext.getInputSchema();
    if (inputSchema == null) {
      throw new Exception("Input schema is null. Input schema can not be null at this stage");
    }
    JavaRDD<Row> map = javaRDD.map(
      structuredRecord -> DataFrames.toRow(structuredRecord, DataFrames.toDataType(inputSchema)));

    SQLContext sqlContext = new SQLContext(sparkExecutionPluginContext.getSparkContext());


    Dataset<Row> data = sqlContext.createDataFrame(map, DataFrames.toDataType(inputSchema));
    WindowSpec spec = Window.partitionBy(config.getPartitionsColumns()).orderBy(config.getPartitionOrderColumns());

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

    List<WindowAggregationConfig.FunctionInfo> aggregatesData = config.getAggregates();


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
      return sparkExecutionPluginContext.getSparkContext()
        .parallelize(rowJavaRDD.collect());
    }

    return sparkExecutionPluginContext.getSparkContext().parallelize(rowJavaRDD.collect(),
                                                                     Integer.parseInt(numberOfPartitionsString));
  }

  private Dataset<Row> apply(WindowAggregationConfig.FunctionInfo data, Dataset<Row> dataFrame,
                             WindowSpec spec) {
    Column aggregateColumn = getAggregateColumn(data).over(spec);
    return dataFrame.withColumn(data.getAlias(), aggregateColumn);
  }

  private Dataset<Row> applyCustomFunction(SQLContext sqlContext, WindowAggregationConfig.FunctionInfo aggregatesDatum,
                                           Dataset<Row> dataFrame, WindowSpec spec, Schema inputSchema) {
    String[] args = aggregatesDatum.getArgs();
    if (args.length < 1) {
      throw new InvalidParameterException("Discrete Percentile must have at least 1 arguments");
    }

    String percentileString = args[0];
    Double percentile;
    try {
      percentile = Double.valueOf(percentileString);
    } catch (NumberFormatException e) {
      throw new InvalidParameterException("Discrete Percentile, first argument must be double value");
    }

    if (percentile > 1 || percentile < 0) {
      throw new InvalidParameterException("Discrete Percentile, percentile must be in range 0.0-1.0");
    }

    Schema.Type type = schemaTypeForInputField(inputSchema, aggregatesDatum.getFieldName());
    DiscretePercentile discretePercentile = new DiscretePercentile(percentile, type);
    sqlContext.udf().register("DISCRETE_PERCENTILE", discretePercentile);

    return apply(aggregatesDatum, dataFrame, spec);
  }

  private Column getAggregateColumn(WindowAggregationConfig.FunctionInfo data) {
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

  private void checkFunctionArguments(String[] args, int minimumArguments, String function) {
    if (args.length < minimumArguments) {
      throw new InvalidParameterException(
        String.format("%s must have at least %s arguments", function, minimumArguments));
    }
  }

  private Schema.Type schemaTypeForInputField(Schema inputSchema, String inputFieldName) {
    if (inputSchema == null) {
      return null;
    }

    Schema.Field schemaField = inputSchema.getField(inputFieldName);
    if (schemaField == null) {
      return null;
    }

    Schema schema = schemaField.getSchema();
    if (schema == null) {
      return null;
    }

    return schema.isNullable() ? schema.getNonNullable().getType() : schema.getType();
  }

  /**
   * Function to map from {@link Row} to {@link StructuredRecord}.
   */
  public static final class RowToRecord implements
    Function<Row, StructuredRecord> {

    private final Schema outputSchema;

    public RowToRecord(Schema outputSchema) {
      this.outputSchema = outputSchema;
    }

    @Override
    public StructuredRecord call(Row row) throws Exception {

      Schema rowSchema = DataFrames.toSchema(row.schema());
      StructuredRecord structuredRecord = DataFrames.fromRow(row, rowSchema);

      if (outputSchema == null || outputSchema.getFields() == null) {
        throw new Exception("Invalid output schema");
      }

      StructuredRecord.Builder builder = StructuredRecord.builder(outputSchema);
      for (Schema.Field field : outputSchema.getFields()) {
        builder.set(field.getName(), structuredRecord.get(field.getName()));
      }
      return builder.build();
    }
  }
}
