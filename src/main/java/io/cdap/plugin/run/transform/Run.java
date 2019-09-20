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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Run Plugin - Runs executable binary installed and available on the Hadoop nodes.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("Run")
@Description("This plugin allows user to run an executable binary installed and available on the local filesystem of" +
  " the Hadoop nodes. Plugin allows the user to read the structured record as input and returns the output record, " +
  "to be further processed downstream in the pipeline.")
public class Run extends Transform<StructuredRecord, StructuredRecord> {
  private final RunConfig config;
  private Schema outputSchema;
  private RunExternalProgramExecutor executor;
  private List<String> inputFieldsToProcess = new ArrayList<String>();

  public Run(RunConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();
    FailureCollector failureCollector = stageConfigurer.getFailureCollector();
    config.validate(failureCollector, inputSchema);

    stageConfigurer.setOutputSchema(buildOutputSchema(inputSchema));
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    if (!Strings.isNullOrEmpty(config.getFieldsToProcess())) {
      for (String inputField : Splitter.on(',').trimResults().split(config.getFieldsToProcess())) {
        inputFieldsToProcess.add(inputField);
      }
    }
    executor = new RunExternalProgramExecutor(config.getCommandToExecute());
    executor.startAndWait();
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {
    outputSchema = outputSchema == null ? buildOutputSchema(structuredRecord.getSchema()) : outputSchema;
    StringBuilder executableInputs = new StringBuilder();

    for (String inputField : inputFieldsToProcess) {
      Schema inputFieldSchema = structuredRecord.getSchema().getField(inputField).getSchema();
      Schema.Type inputFieldType = inputFieldSchema.isNullable() ? inputFieldSchema.getNonNullable().getType() :
        inputFieldSchema.getType();

      if ((inputFieldType.equals(Schema.Type.STRING)) &&
        ((String) (structuredRecord.get(inputField))).contains(" ")) {
        executableInputs.append("\"");
        executableInputs.append((String) structuredRecord.get(inputField));
        executableInputs.append("\"");
      } else {
        executableInputs.append((String) structuredRecord.get(inputField));
      }
      executableInputs.append(" ");
    }

    // append the fixed arguments in the command, if any
    if (!Strings.isNullOrEmpty(config.getFixedInputs())) {
      executableInputs.append(config.getFixedInputs());
    }

    executor.submit(executableInputs.toString().trim(), emitter, structuredRecord, outputSchema);
  }

  @Override
  public void destroy() {
    executor.stopAndWait();
  }

  /**
   * Builds the emitter's final output schema using the output field provided, along with the input fields.
   *
   * @param schema
   * @return output schema
   */
  private Schema buildOutputSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields());
    // Since binary to be executed, can produce null the output for a particular input, hence creating nullable schema
    // for the output field
    Schema.Field field = Schema.Field.of(config.getOutputField(), Schema.nullableOf(Schema.of(Schema.Type.valueOf(
      config.getOutputFieldType().trim().toUpperCase()))));
    if (fields.contains(field)) {
      throw new IllegalArgumentException(String.format("Field %s already has type specified. Duplicate field %s found.",
                                                       config.getOutputField(), config.getOutputField()));
    } else {
      fields.add(field);
    }
    return Schema.recordOf("output-schema", fields);
  }
}
