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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.aggregation.WindowAggregationDefinition;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Engine;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.Relation;
import io.cdap.cdap.etl.api.relational.RelationalTranformContext;

import io.cdap.cdap.etl.proto.validation.SimpleFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;

public class WindowAggregationTest {
  List<String> partitionFields;
  String partitionOrderFields;
  List<WindowAggregationConfig.FunctionInfo> functionInfos;
  WindowAggregationConfig.WindowFrameType windowFrameType;
  @Mock
  private Engine engine;
  @Mock
  private ExpressionFactory<String> expressionFactory;
  private FailureCollector failureCollector;
  @Mock
  private RelationalTranformContext relationalTranformContext;
  @Mock
  private Relation relation;
  private Schema schema;
  @Mock
  private WindowAggregationConfig config;
  public WindowAggregation windowAggregation;
  private WindowAggregationConfig.FunctionInfo functionInfo;
  private WindowAggregationConfig.Function function;
  private Set<Capability> set;
  private List<Schema.Field> schemaFields;
  @Before
  public void setUp() {
    failureCollector = new SimpleFailureCollector();
    partitionFields = new ArrayList<>();
    partitionFields.add("field1");
    partitionFields.add("field2");
    partitionOrderFields  = "field1:Ascending";
    long dummy = 0;
    config = mock(WindowAggregationConfig.class);
    function = WindowAggregationConfig.Function.FIRST;
    functionInfo = new WindowAggregationConfig.FunctionInfo("alias",
            function, "field", null, null);
    functionInfos = new ArrayList<>();
    functionInfos.add(functionInfo);
    relationalTranformContext = mock(RelationalTranformContext.class);
    Mockito.when(config.getPartitionFields()).thenReturn(partitionFields);
    Mockito.when(relationalTranformContext.getEngine()).thenReturn(engine);
    Mockito.when(config.getPartitionOrder()).thenReturn(partitionOrderFields);
    Mockito.when(config.getWindowFrameType()).thenReturn(windowFrameType);
    Mockito.when(config.isFrameDefinitionUnboundedPreceding()).thenReturn(false);
    Mockito.when(config.isFrameDefinitionUnboundedFollowing()).thenReturn(false);
    Mockito.when(config.getFrameDefinitionPrecedingBound()).thenReturn(dummy);
    Mockito.when(config.getFrameDefinitionFollowingBound()).thenReturn(dummy);
    Mockito.when(config.getAggregates(failureCollector)).thenReturn(functionInfos);
    expressionFactory = mock(ExpressionFactory.class);
    windowAggregation = new WindowAggregation(config);
    schemaFields = new ArrayList<>();
    Schema.Field field1 = Schema.Field.of("field1", Schema.of(Schema.Type.STRING));
    Schema.Field field2 = Schema.Field.of("field2", Schema.of(Schema.Type.INT));
    schemaFields.add(field1);
    schemaFields.add(field2);
    schema = Schema.recordOf(schemaFields);
    Mockito.when(relationalTranformContext.getOutputSchema()).thenReturn(schema);
    set = new HashSet<>();
    set.add(StandardSQLCapabilities.BIGQUERY);
    Mockito.when(expressionFactory.getCapabilities()).thenReturn(set);
  }

  @Test
  public void testWindowAggregationGenerationFrameTypeNone() {
    windowFrameType = WindowAggregationConfig.WindowFrameType.NONE;
    WindowAggregationDefinition windowAggregationDefinition = windowAggregation.generateAggregationDefinition(
      relationalTranformContext, relation, failureCollector, expressionFactory);
    Assert.assertNotNull(windowAggregationDefinition);
    Assert.assertEquals(failureCollector.getValidationFailures().size(), 0);
  }

  @Test
  public void testWindowAggregationGenerationFrameTypeRow() {
    windowFrameType = WindowAggregationConfig.WindowFrameType.ROW;
    WindowAggregationDefinition windowAggregationDefinition = windowAggregation.generateAggregationDefinition(
      relationalTranformContext, relation, failureCollector, expressionFactory);
    Assert.assertNotNull(windowAggregationDefinition);
    Assert.assertEquals(failureCollector.getValidationFailures().size(), 0);
  }
}

