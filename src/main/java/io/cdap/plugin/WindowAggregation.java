/*
 * Copyright © 2020-2022 Cask Data, Inc.
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
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.batch.SparkCompute;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.api.batch.SparkPluginContext;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.cdap.etl.api.relational.CoreExpressionCapabilities;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.InvalidRelation;
import io.cdap.cdap.etl.api.relational.LinearRelationalTransform;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Window aggregator plugin
 */
@Plugin(type = SparkCompute.PLUGIN_TYPE)
@Name(WindowAggregation.NAME)
@Description("Specify a window over which functions should be applied. \n" +
  "Supports functions: Rank, Dense Rank, Percent Rank, N tile, Row Number, Median," +
  "Continuous Percentile, Lead, Lag, First, Last, Cumulative distribution, Accumulate.")
public class WindowAggregation extends SparkCompute<StructuredRecord, StructuredRecord>
  implements LinearRelationalTransform {

  public static final String NAME = "WindowAggregation";
  // BigQuery specific aggregations
  private static final Map<WindowAggregationConfig.Function, String> functionBQSqlMap =
    new HashMap<WindowAggregationConfig.Function, String>() {{
      put(WindowAggregationConfig.Function.RANK, "RANK()");
      put(WindowAggregationConfig.Function.DENSE_RANK, "DENSE_RANK()");
      put(WindowAggregationConfig.Function.PERCENT_RANK, "PERCENT_RANK()");
      put(WindowAggregationConfig.Function.N_TILE, "NTILE(%s)");
      put(WindowAggregationConfig.Function.ROW_NUMBER, "ROW_NUMBER()");
      put(WindowAggregationConfig.Function.MEDIAN, "PERCENTILE_CONT(%s, 0.5)");
      put(WindowAggregationConfig.Function.CONTINUOUS_PERCENTILE,
        "PERCENTILE_CONT(%s,%s) OVER()");
      put(WindowAggregationConfig.Function.DISCRETE_PERCENTILE,
        "PERCENTILE_DISC(%s,%s) OVER()");
      put(WindowAggregationConfig.Function.LEAD, "LEAD(%s)");
      put(WindowAggregationConfig.Function.LAG, "LAG(%s)");
      put(WindowAggregationConfig.Function.FIRST, "FIRST_VALUE(%s)");
      put(WindowAggregationConfig.Function.LAST, "LAST_VALUE(%s)");
      put(WindowAggregationConfig.Function.CUMULATIVE_DISTRIBUTION, "CUME_DIST()");
      put(WindowAggregationConfig.Function.ACCUMULATE, "SUM(%s)");
    }};
  private static final Logger LOG = LoggerFactory.getLogger(WindowAggregation.class);
  private final WindowAggregationConfig config;
  private Schema outputSchema;
  private FailureCollector failureCollector;

  public WindowAggregation(WindowAggregationConfig config) {
    this.config = config;
  }

  private static Optional<ExpressionFactory<String>> getExpressionFactory(RelationalTranformContext ctx,
                                                                          WindowAggregationConfig config,
                                                                          FailureCollector failureCollector) {
    List<WindowAggregationConfig.FunctionInfo> functionInfos = config.getAggregates(failureCollector);

    for (WindowAggregationConfig.FunctionInfo aggregate : functionInfos) {
      WindowAggregationConfig.Function func = aggregate.getFunction();
      // If the function is not supported in BigQuery, this relation is not supported by this engine.
      if (!functionBQSqlMap.containsKey(func)) {
        return Optional.empty();
      }
    }
    Engine e = ctx.getEngine();
    return e.getExpressionFactory(StringExpressionFactoryType.SQL, StandardSQLCapabilities.BIGQUERY);
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    failureCollector = stageConfigurer.getFailureCollector();
    Schema inputSchema = stageConfigurer.getInputSchema();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    if (inputSchema == null || aggregates.isEmpty()) {
      failureCollector.getOrThrowException();
      stageConfigurer.setOutputSchema(null);
      return;
    }

    validate(inputSchema, failureCollector);
    failureCollector.getOrThrowException();
    stageConfigurer.setOutputSchema(getOutputSchema(inputSchema, aggregates));
  }

  private void validate(Schema inputSchema, FailureCollector collector) {
    List<String> partitionFields = config.getPartitionFields();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    List<String> partitionOrderFields = config.getPartitionOrderFields();
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
            String.format("Invalid aggregate %s: Field '%s' does not exist in input schema."
              , functionInfo.description(), functionFieldName),
            String.format("Field '%s' must exist in input schema.", functionFieldName))
          .withConfigProperty(WindowAggregationConfig.NAME_AGGREGATES);
      } else {
        inputFieldSchema = inputField.getSchema();
      }

      WindowAggregationConfig.Function function = functionInfo.getFunction();
      //check input schema
      Schema allowedInputSchema = function.getAllowedInputSchema();


      if (allowedInputSchema != null && inputFieldSchema != null) {
        Schema.Type type = inputFieldSchema.isNullable() ? inputFieldSchema.getUnionSchema(0).getType() :
          inputFieldSchema.getType();
        int indexOf = allowedInputSchema.getUnionSchemas().stream().map(Schema::getType)
          .collect(Collectors.toList()).indexOf(type);
        if (indexOf < 0) {
          collector.addFailure(
            String.format("Invalid input schema type '%s' for field '%s' in function '%s'.",
              type, inputField.getName(), function.name()),
            String.format("Allowed input types for function '%s' are '%s'.",
              function.name(), Joiner.on(",").join(allowedInputSchema.getUnionSchemas())));
        }
      }

      if (function.getOutputSchema() == null && inputFieldSchema != null) {
        function.setOutputSchema(inputFieldSchema);
      }
    }

    for (String partitionOrderField : partitionOrderFields) {
      String[] split = partitionOrderField.split(":");
      if (split.length != 2 || split[0] == null || split[1] == null || split[1].length() == 0) {
        collector.addFailure(
          String.format("Column name for order or order type value is missing for the field %s ",
            partitionOrderField), "").withConfigProperty(WindowAggregationConfig.NAME_PARTITION_ORDER);
      }
      Schema.Field field = inputSchema.getField(split[0]);
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
    validate(context.getInputSchema(), failureCollector);
    failureCollector.getOrThrowException();
    Schema outputSchema = getOutputSchema(context.getInputSchema(), aggregates);
    recordLineage(context, outputSchema);
  }

  @Override
  public void initialize(SparkExecutionPluginContext context) throws Exception {
    super.initialize(context);
    FailureCollector failureCollector = context.getFailureCollector();
    List<WindowAggregationConfig.FunctionInfo> aggregates = config.getAggregates(failureCollector);
    validate(context.getInputSchema(), failureCollector);
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
        Collections.singletonList(functionInfo.getFieldName()), functionInfo.getAlias());
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
  public JavaRDD<StructuredRecord> transform(SparkExecutionPluginContext sparkExecutionPluginContext,
                                             JavaRDD<StructuredRecord> javaRDD) throws Exception {
    Schema inputSchema = sparkExecutionPluginContext.getInputSchema();
    if (inputSchema == null) {
      throw new Exception("Input schema is null. Input schema can not be null at this stage");
    }
    return WindowsAggregationUtil.transform(sparkExecutionPluginContext, javaRDD, config, inputSchema, outputSchema);
  }

  @Override
  public boolean canUseEngine(Engine engine) {
    Optional<ExpressionFactory<String>> expressionFactory = engine.
      getExpressionFactory(StringExpressionFactoryType.SQL, StandardSQLCapabilities.BIGQUERY);
    return expressionFactory.isPresent();
  }

  /**
   * Returns generated window aggregation definition after assigning values to all the properties
   *
   * @param ctx RelationalTranformContext to get the output schema
   * @param relation Relation to get the column name of the given field
   * @param failureCollector FailureCollector to add validation failures
   * @return expressionFactory ExpressionFactory to get the column name and expression
   * returned can be null or generated window aggregation definition object.
   */
  @Nullable
  public WindowAggregationDefinition generateAggregationDefinition(RelationalTranformContext ctx, Relation relation,
                                                                   FailureCollector failureCollector,
                                                                   ExpressionFactory<String> expressionFactory) {
    WindowAggregationConfig.WindowFrameType windowFrameType = config.getWindowFrameType();
    WindowAggregationDefinition.WindowFrameType windowFrameTypeValue = null;
    boolean unboundedPreceding = false;
    boolean unboundedFollowing = false;
    String preceding = null;
    String following = null;

    if (windowFrameType != WindowAggregationConfig.WindowFrameType.NONE) {
      unboundedPreceding = config.isFrameDefinitionUnboundedPreceding();
      unboundedFollowing = config.isFrameDefinitionUnboundedFollowing();
      preceding = String.valueOf(config.getFrameDefinitionPrecedingBound());
      following = String.valueOf(config.getFrameDefinitionFollowingBound());
      if (windowFrameType == WindowAggregationConfig.WindowFrameType.ROW) {
        windowFrameTypeValue = WindowAggregationDefinition.WindowFrameType.ROW;
      } else if (windowFrameType == WindowAggregationConfig.WindowFrameType.RANGE) {
        windowFrameTypeValue = WindowAggregationDefinition.WindowFrameType.RANGE;
      }
    }

    List<Expression> partitionExpressions = setPartitionExpressions(config.getPartitionFields(), relation,
      expressionFactory);
    Map<String, Expression> aggregateExpressions = setAggregateExpressions(expressionFactory, relation,
                                                                           failureCollector);
    Map<String, Expression> selectExpressions = setSelectExpressions(ctx, expressionFactory, relation,
                                                                     aggregateExpressions);
    if (partitionExpressions == null || aggregateExpressions == null || selectExpressions == null ||
      partitionExpressions.size() == 0 || aggregateExpressions.size() == 0 || selectExpressions.size() == 0) {
      return null;
    }
    List<WindowAggregationDefinition.OrderByExpression> orderExpressions =
      setOrderByExpressions(config.getPartitionOrderFields(), expressionFactory);
    WindowAggregationDefinition.Builder builder = WindowAggregationDefinition.builder()
      .select(selectExpressions)
      .partition(partitionExpressions)
      .aggregate(aggregateExpressions)
      .orderBy(orderExpressions)
      .windowFrameType(windowFrameTypeValue);
    if (windowFrameTypeValue != WindowAggregationDefinition.WindowFrameType.NONE) {
      builder.unboundedPreceding(unboundedPreceding)
        .unboundedFollowing(unboundedFollowing)
        .preceding(preceding)
        .following(following);
    }
    return builder.build();
  }

  /**
   * Returns Partition Expressions which are set from partition fields of Windows Aggregation Config
   *
   * @param partitionFields PartitionFields to set to Window aggregation Definition
   * @param relation Relation to get the column name of the given field
   * @return the partitionexpressions to set to Window aggregation definition.The list
   * returned can never be empty.
   */
  private List<Expression> setPartitionExpressions(List<String> partitionFields, Relation relation,
                                                   ExpressionFactory<String> expressionFactory) {
    List<Expression> partitionExpressions = new ArrayList<>(partitionFields.size());
    for (String field : partitionFields) {
      String columnName = getColumnName(expressionFactory, relation, field);
      Expression partitionExpression = expressionFactory.compile(columnName);
      partitionExpressions.add(partitionExpression);
    }
    return partitionExpressions;
  }

  /**
   * Returns Order By Expressions which are set from partition order fields of Windows Aggregation Config
   *
   * @param partitionOrderFields PartitionOrderFields to set to Window aggregation Definition
   * @param expressionFactory ExpressionFactory to get the column name of the given field
   * @return the orderExpressions to set to Window aggregation definition.The list
   * returned can be empty.
   */
  private List<WindowAggregationDefinition.OrderByExpression> setOrderByExpressions(List<String> partitionOrderFields,
    ExpressionFactory<String> expressionFactory) {
    List<WindowAggregationDefinition.OrderByExpression> orderExpressions = new ArrayList<>();
    for (String partitionOrderField : partitionOrderFields) {
      String[] split = partitionOrderField.split(":");
      WindowAggregationDefinition.OrderByExpression orderByExpression =
        new WindowAggregationDefinition.OrderByExpression(expressionFactory.compile(split[0].trim()),
          WindowAggregationDefinition.OrderBy.valueOf(split[1].trim().toUpperCase()));
      orderExpressions.add(orderByExpression);
    }
    return orderExpressions;
  }

  /**
   * Returns Select Expressions to set in Window aggregation definition selectExpressions
   *
   * @param ctx RelationalTransformContext for transformation
   * @param expressionFactory ExpressionFactory to get the column name of the given field
   * @param relation Relation to get the column name of the given field
   * @return the orderExpressions to set to Window aggregation definition.The list
   * returned can be empty.
   */
  private Map<String, Expression> setSelectExpressions(RelationalTranformContext ctx,
    ExpressionFactory<String> expressionFactory, Relation relation, Map<String, Expression> aggregateExpressions) {
    Map<String, Expression> selectExpressions = new HashMap<>();
    for (Schema.Field field : Objects.requireNonNull(ctx.getOutputSchema().getFields())) {
      // This condition is to ensure only aggregate functions names are not in select fields
      if (!aggregateExpressions.containsKey(field.getName())) {
        selectExpressions.put(field.getName(), expressionFactory.compile(getColumnName(
          expressionFactory, relation, field.getName())));
      }
    }
    return selectExpressions;
  }

  private Map<String, Expression> setAggregateExpressions(ExpressionFactory<String> expressionFactory,
                                                          Relation relation, FailureCollector failureCollector) {
    Map<String, Expression> aggregateExpressions = new HashMap<>();
    for (WindowAggregationConfig.FunctionInfo aggregate : config.getAggregates(failureCollector)) {
      String alias = aggregate.getAlias();
      String columnName = getColumnName(expressionFactory, relation, aggregate.getFieldName());
      WindowAggregationConfig.Function function = aggregate.getFunction();
      // Check if this function is supported in BigQuery.
      if (!(functionBQSqlMap.containsKey(function)
        && expressionFactory.getCapabilities().contains(StandardSQLCapabilities.BIGQUERY))) {
        failureCollector.addFailure(String.format("BigQuery capability does not exist for function %s", alias)
          , null);
      }
      String selectSql = String.format(functionBQSqlMap.get(function), columnName);
      aggregateExpressions.put(alias, expressionFactory.compile(selectSql));
      continue;
    }
    return aggregateExpressions;
  }

  @Override
  public Relation transform(RelationalTranformContext relationalTranformContext, Relation relation) {
    FailureCollector failureCollector = new SimpleFailureCollector();
    // If the expression factory is not present, this aggregation cannot be handled by the plugin.
    Optional<ExpressionFactory<String>> expressionFactory = getExpressionFactory(relationalTranformContext,
      config, failureCollector);

    // If the expression factory is not present, this aggregation cannot be handled by the plugin.
    if (!expressionFactory.isPresent()) {
      return new InvalidRelation("Expression factory is not present");
    }

    // Check if this aggregation definition is supported in SQL
    WindowAggregationDefinition windowAggregationDefinition = generateAggregationDefinition(relationalTranformContext,
      relation, failureCollector, expressionFactory.get());

    if (windowAggregationDefinition == null) {
      return new InvalidRelation("Unsupported aggregation definition");
    }
    return relation.window(windowAggregationDefinition);
  }

  private String getColumnName(ExpressionFactory<String> expressionFactory, Relation relation, String name) {
    // If the column name is *, return as such.
    if ("*".equals(name)) {
      return name;
    }

    // Verify if the expression factory can provide a quoted column name, and use this if available.
    if (expressionFactory.getCapabilities().contains(CoreExpressionCapabilities.CAN_GET_QUALIFIED_COLUMN_NAME)) {
      return expressionFactory.getQualifiedColumnName(relation, name).extract();
    }

    // Return supplied column name.
    return name;
  }
}
