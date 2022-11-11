/*
 * Copyright © 2022 Cask Data, Inc.
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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.spark.sql.DataFrames;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.batch.SparkExecutionPluginContext;
import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import io.cdap.plugin.function.DiscretePercentile;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.UDFRegistration;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

import static io.cdap.cdap.api.data.schema.Schema.Field;
import static io.cdap.cdap.api.data.schema.Schema.recordOf;

public class WindowsAggregationUtilTest {
  List<WindowAggregationConfig.FunctionInfo> functionInfos;
  private WindowAggregationConfig.FunctionInfo functionInfo;
  private WindowAggregationConfig.Function function;
  @Mock
  private SparkExecutionPluginContext sparkExecutionPluginContext;
  @Mock
  private WindowAggregationConfig config;
  @Mock
  private JavaRDD<StructuredRecord> javaRDDSR;
  @Mock
  private JavaRDD javaRDDRow;
  @Mock
  private JavaSparkContext javaSparkContext;
  @Mock private SparkContext sparkContext;
  @Mock private Dataset data;
  @Mock private StructType structSchema;
  @Mock private FailureCollector failureCollector;
  @Mock private UDFRegistration udfRegistration;
  @Mock private UserDefinedAggregateFunction userDefinedAggregateFunction;
  private Schema schema;

  @Before
  public void setUp() {
    failureCollector = Mockito.mock(SimpleFailureCollector.class);
    function = WindowAggregationConfig.Function.FIRST;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos = new ArrayList<>();
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.DISCRETE_PERCENTILE;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.RANK;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.DENSE_RANK;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.PERCENT_RANK;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.ACCUMULATE;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.ROW_NUMBER;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.MEDIAN;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.CONTINUOUS_PERCENTILE;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.LEAD;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field", new String[]{"1"},
      null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.LAG;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias",
                                                            function, "field", new String[]{"1"}, null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.LAST;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias",
                                                            function, "field", new String[]{"1"}, null);
    functionInfos.add(functionInfo);
    function = WindowAggregationConfig.Function.CUMULATIVE_DISTRIBUTION;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias",
                                                            function, "field", new String[]{"1"}, null);
    functionInfos.add(functionInfo);
    config = Mockito.mock(WindowAggregationConfig.class);

    Mockito.when(config.getAggregates(failureCollector)).thenReturn(functionInfos);
    data = Mockito.mock(Dataset.class);
    javaRDDRow = Mockito.mock(JavaRDD.class);
    javaRDDSR = Mockito.mock(JavaRDD.class);
    javaSparkContext = Mockito.mock(JavaSparkContext.class);
    sparkExecutionPluginContext = Mockito.mock(SparkExecutionPluginContext.class);
    Mockito.when(data.javaRDD()).thenReturn(javaRDDRow);
    Mockito.when(javaRDDSR.map(Mockito.any(Function.class))).thenReturn(javaRDDRow);
    Mockito.when(javaRDDRow.map(Mockito.any(RowToRecord.class))).thenReturn(javaRDDSR);
    Mockito.when(sparkExecutionPluginContext.getSparkContext()).thenReturn(javaSparkContext);
    Mockito.when(sparkExecutionPluginContext.getFailureCollector()).thenReturn(failureCollector);
    sparkContext = Mockito.mock(SparkContext.class);
    Mockito.when(javaSparkContext.sc()).thenReturn(sparkContext);
    List<Field> schemaFields = new ArrayList<>();
    Field field = Field.of("field", Schema.of(Schema.Type.INT));
    schemaFields.add(field);
    schema = recordOf("test", schemaFields);

    Mockito.when(data.withColumn(Mockito.anyString(), Mockito.any(Column.class))).thenReturn(data);
    structSchema = Mockito.mock(StructType.class);
    Mockito.when(config.getWindowFrameType()).thenReturn(WindowAggregationConfig.WindowFrameType.ROW);
    Mockito.when(config.getFrameDefinitionPrecedingBound()).thenReturn(1L);
    Mockito.when(config.getFrameDefinitionFollowingBound()).thenReturn(2L);
    Mockito.when(data.withColumn(Mockito.anyString(), Mockito.nullable(Column.class))).thenReturn(data);
    udfRegistration = Mockito.mock(UDFRegistration.class);
    userDefinedAggregateFunction = Mockito.mock(UserDefinedAggregateFunction.class);
    Mockito.when(udfRegistration.register(Mockito.anyString(), Mockito.any(DiscretePercentile.class)))
      .thenReturn(userDefinedAggregateFunction);
    Mockito.when(config.getNumberOfPartitions()).thenReturn("1");
    Mockito.when(javaRDDSR.repartition(Mockito.anyInt())).thenReturn(javaRDDSR);
  }

  @Test
  public void testTransform() {
     try (MockedConstruction<SQLContext> mocked = Mockito.mockConstruction(SQLContext.class,
      (mock, context) -> {
        Mockito.when(mock.createDataFrame(javaRDDRow, structSchema)).thenReturn(data);
        Mockito.when(mock.udf()).thenReturn(udfRegistration);
        Mockito.mockStatic(DataFrames.class).when(DataFrames.toDataType(schema)).thenReturn(structSchema);
     })) {
       JavaRDD<StructuredRecord> result = WindowsAggregationUtil.transform(sparkExecutionPluginContext, javaRDDSR,
         config, schema, schema);
       Assert.assertNotNull(result);
     } catch (Exception e) {
       Assert.fail("Exception not expected");
     }
  }

  @Test(expected = InvalidParameterException.class)
  public void testInvalidParameter() {
    try (MockedConstruction<SQLContext> mocked = Mockito.mockConstruction(SQLContext.class,
      (mock, context) -> {
        Mockito.when(mock.createDataFrame(javaRDDRow, structSchema)).thenReturn(data);
        Mockito.when(mock.udf()).thenReturn(udfRegistration);
        Mockito.mockStatic(DataFrames.class).when(DataFrames.toDataType(schema)).thenReturn(structSchema);
      })) {
      function = WindowAggregationConfig.Function.N_TILE;
      functionInfo = new WindowAggregationConfig.FunctionInfo("alias", function, "field",
        new String[]{}, null);
      functionInfos.add(functionInfo);
      JavaRDD<StructuredRecord> result = WindowsAggregationUtil.transform(sparkExecutionPluginContext, javaRDDSR,
                                                                          config, schema, schema);
    }
  }
}
