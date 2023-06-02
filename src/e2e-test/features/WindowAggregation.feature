#
# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#

@WINDOW_AGGREGATION_TEST
Feature: Test window aggregation plugin

  @HAS_FILE_SOURCE @HAS_FILE_SINK
  Scenario: Window aggregation works as expected
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "File" from the plugins list as: "Source"
    And Expand Plugin group in the LHS plugins list: "Sink"
    And Select plugin: "File" from the plugins list as: "Sink"
    And Expand Plugin group in the LHS plugins list: "Analytics"
    And Select plugin: "Window Aggregation" from the plugins list as: "Analytics"
    Then Connect plugins: "File" and "Window Aggregation" to establish connection
    And Connect plugins: "Window Aggregation" and "File2" to establish connection

    Then Navigate to the properties page of plugin: "File"
    And Enter input plugin property: "referenceName" with value: "window_aggregation_source"
    And Enter input plugin property: "path" with value: "sourceFilePath"
    And Select dropdown plugin property: "format" with option value: "csv"
    And Click plugin property: "switch-skipHeader"
    And Click on the Get Schema button
    And Verify the Output Schema matches the Expected Schema: "sourceOutputSchema"
    And Validate "File" plugin properties
    And Close the Plugin Properties page

    Then Navigate to the properties page of plugin: "Window Aggregation"
    And Enter in partition fields input: "profession"
    And Enter in order by field key: "age" value: "Descending"
    And Enter in aggregates field key: "age:first(age,1,true)"
    And Validate "Window Aggregation" plugin properties
    And Close the Plugin Properties page

    Then Navigate to the properties page of plugin: "File2"
    And Enter input plugin property: "referenceName" with value: "window_aggregation_test_sink"
    And Enter input plugin property: "path" with value: "sinkFilePath"
    And Replace input plugin property: "suffix" with value: "sinkFileSuffix"
    And Select dropdown plugin property: "format" with option value: "csv"
    And Validate "File2" plugin properties
    And Close the Plugin Properties page

    Then Save the pipeline
    And Deploy the pipeline
    And Run the Pipeline in Runtime
    And Wait for pipeline to be in status: "Succeeded" with a timeout of 300 seconds
    And Validate output records in output folder path "sinkFilePath" is equal to expected output file "expectedOutputPath"
