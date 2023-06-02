/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.plugin.stepdesign;

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.plugin.utils.WindowAggregationActions;
import io.cucumber.java.en.Then;

import java.io.IOException;

/**
 * Window Aggregation plugin related StepsDesign.
 */

public class WindowAggregationSteps implements CdfHelper {

  @Then("Enter in partition fields input: {string}")
  public void enterInPartitionFieldsInput(String partitionOn) {
    WindowAggregationActions.enterInPartitionFields(partitionOn);
  }

  @Then("Enter in order by field key: {string} value: {string}")
  public void enterInOrderByFieldKey(String orderBy, String orderMode) {
    WindowAggregationActions.enterInOrderByFieldKey(orderBy, orderMode);
  }

  @Then("Enter in aggregates field key: {string}")
  public void enterInAggregatesFieldKey(String aggregator) {
    WindowAggregationActions.enterInAggregatesFieldKey(aggregator);
  }

  @Then("Validate output records in output folder path {string} is equal to expected output file {string}")
  public void validateOutputRecordsInOutputFolderIsEqualToExpectedOutputFile(String outputFolder
      , String expectedOutputFile) throws IOException {
    WindowAggregationActions.validateOutputRecordsInSinkFile(outputFolder, expectedOutputFile);
  }
}
