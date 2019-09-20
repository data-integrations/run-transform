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

package io.cdap.plugin.run.transform;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Test cases for RunConfig.
 */
public class RunConfigTest {

  private static final Schema inputSchema = Schema.recordOf("input-record",
                                                            Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                                            Schema.Field.of("input", Schema.of(Schema.Type.STRING)));
  private static final String MOCK_STAGE = "mockStage";
  private static final RunConfig VALID_CONFIG = new RunConfig(
    "java -jar /home/user/Example.jar",
    "input",
    "50 true",
    "output",
    "string",
    null
  );

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, inputSchema);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testInvalidCommandToExecute() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    RunConfig config = RunConfig.builder(VALID_CONFIG)
      .setCommandToExecute("java -jar")
      .build();
    List<List<String>> paramName = Arrays.asList(
      Collections.singletonList(RunConfig.COMMAND_TO_EXECUTE),
      Collections.singletonList(RunConfig.COMMAND_TO_EXECUTE));

    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testInvalidBinaryType() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    RunConfig config = RunConfig.builder(VALID_CONFIG)
      .setCommandToExecute("java -jar /home/user/Example.dll")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RunConfig.COMMAND_TO_EXECUTE));

    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testInvalidInputField() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    RunConfig config = RunConfig.builder(VALID_CONFIG)
      .setFieldsToProcess("invalid_field")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RunConfig.FIELDS_TO_PROCESS));

    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testInvalidOutputFieldType() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    RunConfig config = RunConfig.builder(VALID_CONFIG)
      .setOutputFieldType("record")
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RunConfig.OUTPUT_FIELD_TYPE));

    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, paramName);
  }

  @Test
  public void testInvalidOutputFieldTypeInSchema() {
    Schema outputSchema = Schema.recordOf("input-record",
                                          Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("input", Schema.of(Schema.Type.STRING)),
                                          Schema.Field.of("output", Schema.of(Schema.Type.STRING)));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    RunConfig config = RunConfig.builder(VALID_CONFIG)
      .setSchema(outputSchema.toString())
      .build();
    List<List<String>> paramName = Collections.singletonList(
      Collections.singletonList(RunConfig.OUTPUT_FIELD));

    config.validate(failureCollector, inputSchema);
    assertValidationFailed(failureCollector, paramName);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, List<List<String>> paramNames) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();
    Assert.assertEquals(paramNames.size(), failureList.size());
    Iterator<List<String>> paramNameIterator = paramNames.iterator();
    failureList.stream().map(failure -> failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(CauseAttributes.STAGE_CONFIG) != null)
      .collect(Collectors.toList()))
      .filter(causeList -> paramNameIterator.hasNext())
      .forEach(causeList -> {
        List<String> parameters = paramNameIterator.next();
        Assert.assertEquals(parameters.size(), causeList.size());
        IntStream.range(0, parameters.size()).forEach(i -> {
          ValidationFailure.Cause cause = causeList.get(i);
          Assert.assertEquals(parameters.get(i), cause.getAttribute(CauseAttributes.STAGE_CONFIG));
        });
      });
  }
}
