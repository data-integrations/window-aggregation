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
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    Schema inputSchema = stageConfigurer.getInputSchema();

    List<String> partitionFields = config.getPartitionFields();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    List<String> partitionOrderFields = config.getPartitionOrderFields();

    if (inputSchema == null || partitionFields.isEmpty() || aggregates.isEmpty()) {
      failureCollector.getOrThrowException();
      stageConfigurer.setOutputSchema(null);
      return;
    }

    validate(inputSchema, partitionFields, aggregates, partitionOrderFields, failureCollector);
    failureCollector.getOrThrowException();
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, aggregates));
  }

  private void validate(Schema inputSchema, List<String> partitionFields,
                        List<WindowAggregationConfig.FunctionInfo> aggregates, List<String> partitionOrderFields,
                        FailureCollector collector) {

    for (String partitionField : partitionFields) {
      Schema.Field field = inputSchema.getField(partitionField);
      if (field == null) {
        collector.addFailure(String.format("Partition field '%s' must exist in input schema.", partitionField),
                             "")
          .withConfigElement(WindowAggregationConfig.NAME_PARTITION_FIELD, partitionField);
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
          .withConfigProperty(WindowAggregationConfig.NAME_AGGREGATES);
      } else {
        inputFieldSchema = inputField.getSchema();
      }

      WindowAggregationConfig.Function function = functionInfo.getFunction();
      //check input schema
      Schema allowedInputSchema = function.getAllowedInputScheme();


      if (allowedInputSchema != null && inputFieldSchema != null) {
        Schema.Type type = inputFieldSchema.isNullable() ? inputFieldSchema.getUnionSchema(0).getType() :
          inputFieldSchema.getType();
        int indexOf = allowedInputSchema.getUnionSchemas().stream().map(Schema::getType).collect(Collectors.toList())
          .indexOf(type);
        if (indexOf < 0) {
          collector.addFailure(
            String.format("Invalid input schema type '%s' for field '%s' in function '%s'.",
                          type, inputField.getName(), function.name())
            , String.format("Allowed input types for function '%s' are '%s'.",
                            function.name(), Joiner.on(",").join(allowedInputSchema.getUnionSchemas())));
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
          withConfigProperty(WindowAggregationConfig.NAME_PARTITION_ORDER);
      }
    }
  }

  private Schema getOutputSchema(Schema inputSchema, List<WindowAggregationConfig.FunctionInfo> aggregates) {
    List<Schema.Field> outputFields = new ArrayList<>(aggregates.size());
    List<Schema.Field> inputSchemaFields = inputSchema.getFields();
    if (inputSchemaFields != null) {
      outputFields.addAll(inputSchemaFields);
    }
    for (WindowAggregationConfig.FunctionInfo aggregate : aggregates) {
      outputFields.add(Schema.Field.of(aggregate.getAlias(), aggregate.getFunction().getOutputSchema()));
    }
    return Schema.recordOf(inputSchema.getRecordName() + ".window", outputFields);
  }

  @Override
  public void prepareRun(SparkPluginContext context) throws Exception {
    super.prepareRun(context);
    FailureCollector failureCollector = context.getFailureCollector();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    validate(context.getInputSchema(), config.getPartitionFields(), aggregates, config.getPartitionOrderFields(),
             failureCollector);
    failureCollector.getOrThrowException();
    Schema outputSchema = getOutputSchema(context.getInputSchema(), aggregates);
    recordLineage(context, outputSchema);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    FailureCollector failureCollector = context.getFailureCollector();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    validate(context.getInputSchema(), config.getPartitionFields(), aggregates, config.getPartitionOrderFields(),
             failureCollector);
    failureCollector.getOrThrowException();
    outputSchema = getOutputSchema(context.getInputSchema(), aggregates);
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
    for (WindowAggregationConfig.FunctionInfo functionInfo : config.getAggregates(context.getFailureCollector())) {

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
    return WindowsAggregationUtil.transform(sparkExecutionPluginContext, javaRDD, config, inputSchema, outputSchema);
  }
}
