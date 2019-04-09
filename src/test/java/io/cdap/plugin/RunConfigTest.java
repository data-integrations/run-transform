/*
 * Copyright © 2016-2019 Cask Data, Inc.
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
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test cases for RunConfig.
 */
public class RunConfigTest extends TransformPluginsTestBase {

  private static final Schema inputSchema = Schema.recordOf("input-record",
                                                            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                                            Schema.Field.of("input", Schema.of(Schema.Type.STRING)));

  @Test
  public void testInvalidCommandToExecute() throws Exception {
    Run.RunConfig config = new Run.RunConfig("java -jar", "input", "50 true", "output", "string", null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new Run(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Error while accessing the binary. Please make sure that the 'Command to Execute' is in " +
                            "the expected format. 'java -jar'", e.getMessage());
    }
  }

  @Test
  public void testInvalidBinaryType() throws Exception {
    Run.RunConfig config = new Run.RunConfig("java -jar /home/user/Example.dll", "input", "50 true", "output",
                                             "string", null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new Run(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Binary type 'dll' is not supported. Supported executable types are: 'exe, sh, bat and jar'" +
                            ".", e.getMessage());
    }
  }

  @Test
  public void testInvalidInputField() throws Exception {
    Run.RunConfig config = new Run.RunConfig("java -jar /home/user/Example.jar", "invalid_field", "50 true", "output",
                                             "string", null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new Run(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Input field 'invalid_field' does not exist in the input schema: '{\"type\":\"record\"," +
                            "\"name\":\"input-record\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},{\"name\":" +
                            "\"input\",\"type\":\"string\"}]}'.",
                          e.getMessage());
    }
  }

  @Test
  public void testInvalidOutputFieldType() throws Exception {
    Run.RunConfig config = new Run.RunConfig("java -jar /home/user/Example.jar", "input", "50 true", "output",
                                             "record", null);
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new Run(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Schema type 'record' for output field is not supported. Supported types are: ' boolean, " +
                            "bytes, double, float, int, long and string.", e.getMessage());
    }
  }

  @Test
  public void testInvalidOutputFieldTypeInSchema() throws Exception {
    Schema outputSchema = Schema.recordOf("input-record",
                                          Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("input", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("output", Schema.of(Schema.Type.STRING)));

    Run.RunConfig config = new Run.RunConfig("java -jar /home/user/Example.jar", "input", "50 true", "output",
                                             "string", outputSchema.toString());
    MockPipelineConfigurer configurer = new MockPipelineConfigurer(inputSchema);
    try {
      new Run(config).configurePipeline(configurer);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertEquals("Output Field 'output' should be of nullable type. Please check the output schema " +
                            "'{\"type\":\"record\",\"name\":\"input-record\",\"fields\":[{\"name\":\"id\"," +
                            "\"type\":\"string\"},{\"name\":\"input\",\"type\":\"string\"},{\"name\":\"output\"," +
                            "\"type\":\"string\"}]}'.", e.getMessage());
    }
  }
}
