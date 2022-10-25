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
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Config for Window aggregation plugin.
 */
public class WindowAggregationConfig extends PluginConfig {

  public static final String NAME_AGGREGATES = "aggregates";
  public static final String NAME_PARTITION_FIELD = "partitionFields";
  public static final String NAME_PARTITION_ORDER = "partitionOrder";

  @Macro
  @Description("Specifies a list of fields, comma-separated, to partition the data by. At least 1 field must be " +
    "provided.")
  private String partitionFields;

  @Macro
  @Nullable
  @Description("Specifies key-value pairs containing the ordering field, and the order "
    + "(ascending or descending). All data types are allowed, except when `Frame Type` is RANGE and " +
    "`Unbounded preceding` or `Unbounded following`is set to `false`, order must be single expression and data type " +
    "must be one of: `Int`, `Long`, `Double`, `Float`. For example: `value:Ascending,id:Descending`")
  private String partitionOrder;

  @Macro
  @Nullable
  @Description("Selects the type of window frame to create within each partition. "
    + "Options can be ROW or RANGE or NONE.")
  private String windowFrameType;

  @Macro
  @Nullable
  @Description("Whether to use an unbounded start boundary for a frame.")
  private Boolean unboundedPreceding;

  @Macro
  @Nullable
  @Description("Whether to use an unbounded end boundary for a frame.")
  private Boolean unboundedFollowing;

  @Macro
  @Nullable
  @Description("Specifies the number of preceding rows in the window frame. When Frame Type is ROW, this is a number" +
    " relative to the current row. E.g. -2 to begin the frame 2 rows before the current row. When Frame Type is RANGE,"
    + "specifies the value to be subtracted from the value of the current row to get the start boundary.")
  private String preceding;

  @Macro
  @Nullable
  @Description("Specifies the number of following rows to include in the window frame. When Frame Type is ROW, this " +
    "is a number relative to the current row. E.g. 3 to end the frame 3 rows after the current row. When Frame Type " +
    "is RANGE, specifies the value to be added to the value of the current row to get the end boundary.")
  private String following;

  @Macro
  @Description("Specifies a list of functions to run on the selected window. " +
    "Supported aggregate functions are Rank, Dense_Rank, Percent_Rank, N_Tile, Row_Number, Median, Continuous " +
    "Percentile, Lead, Lag, First, Last, Cumulative_Distribution, Accumulate. Aggregates are " +
    "specified using syntax: `alias:function(field,encoded(arguments),ignoreNulls)[\n other functions]`." +
    "For example, 'nextValue:lead(value,1,false)\npreviousValue:lag(value,1,false)' will calculate two aggregates. " +
    "The first will create a field called 'nextValue' that is the next value of current row in the group." +
    "The second will create a field called 'previousValue' that is the previous value of current row in the group.")
  private String aggregates;


  @Macro
  @Nullable
  @Description("Number of partitions to use when aggregating. If not specified, the execution "
    + "framework will decide how many to use")
  private String numberOfPartitions;

  @Name("schema")
  @Nullable
  @Description("Specifies the schema of the records outputted from this plugin.")
  private String schema;

  private static Schema numericSchema() {
    return Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.DOUBLE), Schema.of(Schema.Type.LONG),
      Schema.of(Schema.Type.FLOAT));
  }

  public List<String> getPartitionFields() {
    List<String> fields = new ArrayList<>();
    if (containsMacro("partitionFields")) {
      return fields;
    }
    for (String field : Splitter.on(",").trimResults().split(partitionFields)) {
      fields.add(field);
    }
    return fields;
  }

  public String getPartitionOrder() {
    return partitionOrder;
  }

  public List<String> getPartitionOrderFields() {
    List<String> orderFields = new ArrayList<>();
    if (containsMacro("partitionOrder") || Strings.isNullOrEmpty(partitionOrder)) {
      return orderFields;
    }
    for (String fieldAndOrder : Splitter.on(",").trimResults().split(partitionOrder)) {
      orderFields.add(fieldAndOrder);
    }
    return orderFields;
  }

  public WindowFrameType getWindowFrameType() {
    if (windowFrameType == null || windowFrameType.isEmpty()) {
      return WindowFrameType.NONE;
    }
    return WindowFrameType.valueOf(windowFrameType);
  }

  public boolean isFrameDefinitionUnboundedPreceding() {
    return unboundedPreceding != null && unboundedPreceding;
  }

  public boolean isFrameDefinitionUnboundedFollowing() {
    return unboundedFollowing != null && unboundedFollowing;
  }

  public long getFrameDefinitionPrecedingBound() {
    if (isFrameDefinitionUnboundedPreceding()) {
      return Long.MIN_VALUE;
    }
    if (Strings.isNullOrEmpty(preceding)) {
      return 0;
    }
    return Long.parseLong(preceding);
  }

  public long getFrameDefinitionFollowingBound() {
    if (isFrameDefinitionUnboundedFollowing()) {
      return Long.MAX_VALUE;
    }
    if (Strings.isNullOrEmpty(following)) {
      return 0;
    }
    return Long.parseLong(following);
  }

  public List<FunctionInfo> getAggregates(FailureCollector failureCollector) {
    List<FunctionInfo> functionInfos = new ArrayList<>();
    if (containsMacro(NAME_AGGREGATES)) {
      return functionInfos;
    }

    Set<String> aggregateNames = new HashSet<>();

    for (String aggregate : Splitter.on('\n').trimResults().split(aggregates)) {
      int colonIdx = aggregate.indexOf(':');
      if (colonIdx < 0) {
        failureCollector.addFailure(
          String.format("Could not find ':' separating aggregate alias from its function in '%s'.", aggregate),
           "Functions must be specified as alias:function(field, argumentsEncoded, ignoreNulls).")
              .withConfigProperty(NAME_AGGREGATES);
        continue;
      }
      String alias = aggregate.substring(0, colonIdx).trim();
      if (!aggregateNames.add(alias)) {
        failureCollector.addFailure(String.format(
          "Cannot create multiple aggregate functions with the same alias '%s'.", alias),
            "Provided aliases must be unique.")
              .withConfigProperty(NAME_AGGREGATES);
        continue;
      }

      String functionAndParameters = aggregate.substring(colonIdx + 1).trim();
      int firstParameterIndex = functionAndParameters.indexOf('(');
      if (firstParameterIndex < 0) {
        failureCollector.addFailure(String.format("Could not find '(' in function '%s'.", functionAndParameters),
          "Functions must be specified as function(field, argumentsEncoded," +
            " ignoreNulls).").withConfigProperty(NAME_AGGREGATES);
        continue;
      }
      String functionStr = functionAndParameters.substring(0, firstParameterIndex).trim();
      Function function;
      try {
        function = Function.valueOf(functionStr.toUpperCase());
      } catch (IllegalArgumentException e) {
        failureCollector.addFailure(String.format("Invalid function '%s'.", functionStr),
          String.format("Must be one of %s.", Joiner.on(',').join(Function.values())))
            .withConfigProperty(NAME_AGGREGATES);
        continue;
      }

      String parameters = functionAndParameters.substring(firstParameterIndex + 1).trim();
      if (!parameters.endsWith(")")) {
        failureCollector.addFailure(
          String.format("Could not find closing ')' in function '%s'.", functionAndParameters),
           "Functions must be specified as function(field, argumentsEncoded, ignoreNulls).")
             .withConfigProperty(NAME_AGGREGATES);
        continue;
      }

      int firstParameterEndIndex = parameters.indexOf(",");
      if (firstParameterEndIndex < 0) {
        failureCollector.addFailure(String.format("Could not find '(' in function '%s'.", functionAndParameters),
          "Functions must be specified as function(field, argumentsEncoded," +
            " ignoreNulls).")
                .withConfigProperty(NAME_AGGREGATES);
        continue;
      }

      String field = parameters.substring(0, firstParameterEndIndex).trim();

      int secondParameterEndIndex = parameters.indexOf(",", firstParameterEndIndex + 1);

      if (secondParameterEndIndex < 0) {
        failureCollector.addFailure(String.format("Could not find '(' in function '%s'.", functionAndParameters),
           "Functions must be specified as function(field, argumentsEncoded, " +
              "ignoreNulls).").withConfigProperty(NAME_AGGREGATES);
        continue;
      }

      String encodedArguments = parameters.substring(firstParameterEndIndex + 1, secondParameterEndIndex).trim();
      String decodedArguments = encodedArguments.replace("%2C", ",");
      Iterable<String> split = Splitter.on(",").trimResults().split(decodedArguments);
      String[] strings = Iterables.toArray(split, String.class);
      if (strings.length == 1 && strings[0].isEmpty()) {
        strings = new String[0];
      }

      String thirdParameter = parameters.substring(secondParameterEndIndex + 1, parameters.length() - 1);

      FunctionInfo functionInfo = new FunctionInfo(alias, function, field, strings, thirdParameter);
      functionInfos.add(functionInfo);
    }

    if (functionInfos.isEmpty()) {
      failureCollector.addFailure("Missing 'aggregates' property.", "The 'aggregates' property must" +
        " be set.").withConfigProperty(NAME_AGGREGATES);
    }
    return functionInfos;
  }

  public String getSchema() {
    return schema;
  }

  @Nullable
  public String getNumberOfPartitions() {
    return numberOfPartitions;
  }

  /**
   * Field Order Type
   */
  protected enum Order {
    ASCENDING,
    DESCENDING;

    public static Order fromString(String order) {
      return "Ascending".equalsIgnoreCase(order) ? Order.ASCENDING : Order.DESCENDING;
    }
  }

  /**
   * Window Frame Type
   */
  protected enum WindowFrameType {
    NONE,
    ROW,
    RANGE
  }

  enum Function {
    RANK(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.INT))),
    DENSE_RANK(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.INT))),
    PERCENT_RANK(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.FLOAT))),
    N_TILE(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.INT))),
    ROW_NUMBER(null, Schema.nullableOf(Schema.of(Schema.Type.INT))),
    MEDIAN(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    CONTINUOUS_PERCENTILE(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    DISCRETE_PERCENTILE(numericSchema(), null),
    LEAD(null, null),
    LAG(null, null),
    FIRST(null, null),
    LAST(null, null),
    CUMULATIVE_DISTRIBUTION(numericSchema(), Schema.nullableOf(Schema.of(Schema.Type.DOUBLE))),
    ACCUMULATE(numericSchema(), null);

    private Schema allowedInputScheme;
    private Schema outputSchema;

    /**
     * @param allowedInputSchema allowed input field schema type where function can be applied,
     *                           if not defined all schema types are allowed.
     * @param outputSchema       if not defined, null, schema type of input field will be as output schema.
     */
    Function(Schema allowedInputSchema, Schema outputSchema) {
      this.allowedInputScheme = allowedInputSchema;
      this.outputSchema = outputSchema;
    }

    public Schema getAllowedInputScheme() {
      return allowedInputScheme;
    }

    public void setAllowedInputScheme(Schema allowedInputScheme) {
      this.allowedInputScheme = allowedInputScheme;
    }

    public Schema getOutputSchema() {
      return outputSchema;
    }

    public void setOutputSchema(Schema outputSchema) {
      this.outputSchema = outputSchema;
    }
  }

  /**
   * Class for holding parsed information of functions defined in configuration
   */
  public static class FunctionInfo {
    private final Function function;
    private final String fieldName;
    private final String alias;
    private final String[] args;
    private final boolean ignoreNull;

    public FunctionInfo(String alias, Function function, String fieldName, String[] arguments, String ignoreNulls) {
      this.alias = alias;
      this.function = function;
      this.fieldName = fieldName;
      this.args = arguments;
      this.ignoreNull = !"false".equals(ignoreNulls);
    }

    public Function getFunction() {
      return function;
    }

    public String getFieldName() {
      return fieldName;
    }

    public String getAlias() {
      return alias;
    }

    public String[] getArgs() {
      return args;
    }

    public boolean isIgnoreNull() {
      return ignoreNull;
    }

    public String description() {
      return String.format("%s:%s(%s,%s,%s)", getAlias(), getFunction().name(), getFieldName(),
        Joiner.on(",").join(args), ignoreNull);
    }
  }
}
