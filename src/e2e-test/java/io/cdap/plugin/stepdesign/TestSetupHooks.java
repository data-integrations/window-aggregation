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

import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.After;
import io.cucumber.java.Before;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

/**
 * test setup hooks.
 */

public class TestSetupHooks {

  @Before(order = 1, value = "@HAS_FILE_SOURCE")
  public static void setFileAbsolutePaths() {
    String sourcePath = PluginPropertyUtils.pluginProp("sourceFilePath");
    if (!sourcePath.startsWith("/")) {
      PluginPropertyUtils.addPluginProp("sourceFilePath",
          Paths.get(TestSetupHooks.class.getResource("/" + sourcePath).getPath()).toString());
    }

    String sinkPath = PluginPropertyUtils.pluginProp("sinkFilePath");
    if (!sinkPath.startsWith("/")) {
      PluginPropertyUtils.addPluginProp("sinkFilePath", Paths.get(TestSetupHooks.class.getResource
          ("/" + sinkPath).getPath()).toString());
    }

    String expectedPath = PluginPropertyUtils.pluginProp("expectedOutputPath");
    if (!expectedPath.startsWith("/")) {
      PluginPropertyUtils.addPluginProp("expectedOutputPath",
          Paths.get(TestSetupHooks.class.getResource
              ("/" + expectedPath).getPath()).toString());
    }
  }

  @After(order = 1, value = "@HAS_FILE_SINK")
  public static void deleteFileSinkOutputFolder() throws IOException {
    FileUtils.deleteDirectory(new File(PluginPropertyUtils.pluginProp("sinkFilePath")
        + "/" + PluginPropertyUtils.pluginProp("sinkFileSuffix")));
  }
}
