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

package io.cdap.plugin.utils;

import io.cdap.e2e.pages.locators.CdfPluginPropertiesLocators;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumDriver;
import io.cdap.e2e.utils.WaitHelper;
import org.junit.Assert;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Window Aggregtaion plugin related Actions.
 */

public class WindowAggregationActions {

  public static void enterInPartitionFields(String partitionOn) {
    String xpath = "//*[@data-testid='partitionFields']//*[@data-testid= 'key']/input";
    WaitHelper.waitForElementToBePresent(By.xpath(xpath));
    SeleniumDriver.getDriver().findElement(By.xpath(xpath)).sendKeys(partitionOn);
  }

  public static void enterInOrderByFieldKey(String orderBy, String orderMode) {
    String xpath = "//*[@data-testid='partitionOrder']//*[@data-testid= 'key']/input";
    WaitHelper.waitForElementToBePresent(By.xpath(xpath));
    SeleniumDriver.getDriver().findElement(By.xpath(xpath)).sendKeys(orderBy);

    String dropdownXpath = "//*[@data-testid='partitionOrder']//*[@data-testid= 'value']";
    WebElement dropdownEl = SeleniumDriver.getDriver().findElement(By.xpath(dropdownXpath));
    ElementHelper.selectDropdownOption(
        dropdownEl,
        CdfPluginPropertiesLocators.locateDropdownListItem(orderMode));
  }

  public static void enterInAggregatesFieldKey(String aggregator) {
    String xpath = "//*[@data-testid='aggregates']//*[@data-testid= 'key']/input";
    WaitHelper.waitForElementToBePresent(By.xpath(xpath));
    SeleniumDriver.getDriver().findElement(By.xpath(xpath)).sendKeys(aggregator);
  }

  public static void validateOutputRecordsInSinkFile(String outputFolder,
      String expectedOutputFile) throws IOException {
    List<String> expectedOutput = new ArrayList<>();
    int expectedOutputRecordsCount = 0;
    try (BufferedReader bf1 = Files.newBufferedReader(Paths.get(PluginPropertyUtils.pluginProp(expectedOutputFile)))) {
      String line;
      while ((line = bf1.readLine()) != null) {
        expectedOutput.add(line);
        expectedOutputRecordsCount++;
      }

      List<Path> partFiles = Files.walk(Paths.get(PluginPropertyUtils.pluginProp(outputFolder)))
          .filter(Files::isRegularFile)
          .filter(file -> file.toFile().getName().startsWith("part-r")).collect(Collectors.toList());

      int outputRecordsCount = 0;
      List<String> actualOutput = new ArrayList<>();
      for (Path partFile : partFiles) {
        try (BufferedReader bf = Files.newBufferedReader(partFile.toFile().toPath())) {
          String line1;
          while ((line1 = bf.readLine()) != null) {
            if (!(expectedOutput.contains(line1))) {
              Assert.fail("Output records not equal to expected output");
            } else {
              actualOutput.add(line1);
            }
            outputRecordsCount++;
          }
        }
      }
      Assert.assertEquals("Output records count should be equal to expected output records count"
          , expectedOutputRecordsCount, outputRecordsCount);
      Assert.assertEquals("Output records not matching expected records",
          getSortedCSVContent(expectedOutput), getSortedCSVContent(actualOutput));
    }
  }

  private static String getSortedCSVContent(List<String> lst) {
    lst.sort((s1, s2) -> {
      String id1 = s1.split(",")[0], id2 = s2.split(",")[0];
      return Integer.parseInt(id1) - Integer.parseInt(id2);
    });
    StringBuilder sb = new StringBuilder();
    for (String s : lst) {
      sb.append(s);
    }
    return sb.toString();
  }
}
