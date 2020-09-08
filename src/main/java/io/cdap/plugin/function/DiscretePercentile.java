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

package io.cdap.plugin.function;

import io.cdap.cdap.api.data.schema.Schema;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 *  Calculate Discrete Percentile
 */
public class DiscretePercentile extends UserDefinedAggregateFunction {

  private final double percentile;
  private final DataType inputSchemaSparkType;

  public DiscretePercentile(double percentile, Schema.Type inputSchemaType) {
    this.percentile = percentile;
    this.inputSchemaSparkType = convertType(inputSchemaType);
  }

  @Override
  public StructType inputSchema() {
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("value", inputSchemaSparkType, true));
    return DataTypes.createStructType(inputFields);
  }

  private DataType convertType(Schema.Type type) {
    switch (type) {
      case INT:
        return DataTypes.IntegerType;
      case LONG:
        return DataTypes.LongType;
      case FLOAT:
        return DataTypes.FloatType;
      case DOUBLE:
        return DataTypes.DoubleType;
      default: {
        throw new RuntimeException("Only Numeric type are supported.");
      }
    }
  }

  @Override
  public StructType bufferSchema() {
    MapType map = DataTypes.createMapType(inputSchemaSparkType, DataTypes.LongType);
    List<StructField> inputFields = new ArrayList<>();
    inputFields.add(DataTypes.createStructField("buffer_value", map, true));
    return DataTypes.createStructType(inputFields);
  }

  @Override
  public DataType dataType() {
    return inputSchemaSparkType;
  }

  @Override
  public boolean deterministic() {
    return true;
  }

  @Override
  public void initialize(MutableAggregationBuffer buffer) {
    buffer.update(0, new HashMap<Number, Long>());
  }

  @Override
  public void update(MutableAggregationBuffer buffer, Row input) {
    if (input.isNullAt(0)) {
      return;
    }
    Object value = input.get(0);
    Map<Number, Long> map = new HashMap<>(buffer.getJavaMap(0));
    if (map.get(value) == null) {
      map.put((Number) value, 1L);
    } else {
      Long frequency = map.get(value);
      map.put((Number) value, frequency + 1);
    }

    buffer.update(0, map);
  }

  @Override
  public void merge(MutableAggregationBuffer buffer1, Row buffer2) {

    Map<Number, Long> map1 = buffer1.getJavaMap(0);
    Map<Number, Long> map2 = buffer2.getJavaMap(0);

    Map<Number, Long> mergeResult = new HashMap(map1);

    for (Map.Entry<Number, Long> valueBuffer2 : map2.entrySet()) {
      Long value = mergeResult.get(valueBuffer2.getKey());
      if (value == null) {
        mergeResult.put(valueBuffer2.getKey(), valueBuffer2.getValue());
      } else {
        Long value2 = valueBuffer2.getValue();
        mergeResult.put(valueBuffer2.getKey(), value + value2);
      }
    }
    buffer1.update(0, mergeResult);

  }

  @Override
  public Object evaluate(Row buffer) {

    if (buffer.size() == 0 || buffer.isNullAt(0)) {
      return null;
    }

    Map<Number, Long> javaMap = buffer.getJavaMap(0);
    TreeMap<Number, Long> sortedMap = new TreeMap<>(javaMap);
    long count = sortedMap.values().stream().reduce(0L, Long::sum);

    boolean found = false;
    Number result = null;
    Iterator<Map.Entry<Number, Long>> iterator = sortedMap.entrySet().iterator();
    long totalRow = 0;

    while (!found && iterator.hasNext()) {
      Map.Entry<Number, Long> next = iterator.next();
      Long value = next.getValue();
      totalRow = totalRow + value;
      double cumulativeDistribution = totalRow / (double) count;
      if (cumulativeDistribution >= percentile) {
        found = true;
        result = next.getKey();
      }
    }
    return result;
  }
}
