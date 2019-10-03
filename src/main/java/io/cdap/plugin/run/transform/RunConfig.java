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
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Configuration class for Run.
 */
public class RunConfig extends PluginConfig {
  public static final String COMMAND_TO_EXECUTE = "commandToExecute";
  public static final String FIELDS_TO_PROCESS = "fieldsToProcess";
  public static final String FIXED_INPUTS = "fixedInputs";
  public static final String OUTPUT_FIELD = "outputField";
  public static final String OUTPUT_FIELD_TYPE = "outputFieldType";
  public static final String SCHEMA = "schema";

  @Name(COMMAND_TO_EXECUTE)
  @Description("Command that will contain the full path to the executable binary present on the local filesystem of" +
    " the Hadoop nodes as well as how to execute that binary. It should not contain any input arguments. For " +
    "example, java -jar /home/user/ExampleRunner.jar, if the binary to be executed is of type jar.")
  private final String commandToExecute;

  @Name(FIELDS_TO_PROCESS)
  @Description("A comma-separated sequence of the fields that will be passed to the binary through STDIN as an " +
    "varying input. For example, 'firstname' or 'firstname,lastname' in case of multiple inputs. Please make sure " +
    "that the sequence of fields is in the order as expected by binary. (Macro Enabled)")
  @Nullable
  @Macro
  private final String fieldsToProcess;

  @Name(FIXED_INPUTS)
  @Description("A space-separated sequence of the fixed inputs that will be passed to the executable binary through" +
    " STDIN. Please make sure that the sequence of inputs is in the order as expected by binary. All the fixed " +
    "inputs will be followed by the variable inputs, provided through 'Fields to Process for Variable Inputs'. " +
    "(Macro enabled)")
  @Nullable
  @Macro
  private final String fixedInputs;

  @Name(OUTPUT_FIELD)
  @Description("The field name that holds the output of the executable binary.")
  private final String outputField;

  @Name(OUTPUT_FIELD_TYPE)
  @Description("Schema type of the 'Output Field'. Supported types are: boolean, bytes, double, float, int, long " +
    "and string.")
  private final String outputFieldType;

  @Name(SCHEMA)
  @Description("Schema of the record.")
  @Nullable
  private final String schema;

  public RunConfig(String commandToExecute, @Nullable String fieldsToProcess, @Nullable String fixedInputs,
                   String outputField, String outputFieldType, @Nullable String schema) {
    this.commandToExecute = commandToExecute;
    this.fieldsToProcess = fieldsToProcess;
    this.fixedInputs = fixedInputs;
    this.outputField = outputField;
    this.outputFieldType = outputFieldType;
    this.schema = schema;
  }

  private RunConfig(Builder builder) {
    commandToExecute = builder.commandToExecute;
    fieldsToProcess = builder.fieldsToProcess;
    fixedInputs = builder.fixedInputs;
    outputField = builder.outputField;
    outputFieldType = builder.outputFieldType;
    schema = builder.schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(RunConfig copy) {
    return builder()
      .setCommandToExecute(copy.commandToExecute)
      .setFieldsToProcess(copy.fieldsToProcess)
      .setFixedInputs(copy.fixedInputs)
      .setOutputField(copy.outputField)
      .setOutputFieldType(copy.outputFieldType)
      .setSchema(copy.schema);
  }

  public String getCommandToExecute() {
    return commandToExecute;
  }

  @Nullable
  public String getFieldsToProcess() {
    return fieldsToProcess;
  }

  @Nullable
  public String getFixedInputs() {
    return fixedInputs;
  }

  public String getOutputField() {
    return outputField;
  }

  public String getOutputFieldType() {
    return outputFieldType;
  }

  @Nullable
  public String getSchema() {
    return schema;
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    validateBinaryExecutableType(failureCollector);
    validateOutputFieldType(failureCollector);
    if (!Strings.isNullOrEmpty(getSchema())) {
      verifyOutputFieldTypeInSchema(failureCollector);
    }
    if (inputSchema == null) {
      failureCollector.addFailure("Input Schema must be specified.", null);
      return;
    }
    validateInputFields(failureCollector, inputSchema);
  }

  /**
   * Validates whether the binary executable type is supported or not.
   */
  private void validateBinaryExecutableType(FailureCollector failureCollector) {
    String executableExtension = "";
    int separatorPosition = commandToExecute.lastIndexOf('.');
    if (separatorPosition > 0) {
      String extensionStrings[] = commandToExecute.substring(separatorPosition + 1).trim().split(" ");
      executableExtension = extensionStrings[0];
    } else {
      failureCollector.addFailure(
        "Error while accessing the binary.",
        "Make sure that the 'Command to Execute' is in the correct format.")
        .withConfigProperty(COMMAND_TO_EXECUTE);
    }
    switch (executableExtension) {
      case "jar":
      case "sh":
      case "exe":
      case "bat":
        break;
      default:
        failureCollector.addFailure(String.format("Binary type '%s' is not supported.", executableExtension),
                                    "Supported executable types are: 'exe, sh, bat and jar'.")
          .withConfigProperty(COMMAND_TO_EXECUTE);
    }
  }

  /**
   * Validates whether the input field to process, is present in input schema or not.
   *
   * @param inputSchema
   */
  private void validateInputFields(FailureCollector failureCollector, Schema inputSchema) {
    if (!containsMacro(fieldsToProcess) && !Strings.isNullOrEmpty(fieldsToProcess)) {
      for (String inputField : Splitter.on(',').trimResults().split(fieldsToProcess)) {
        if (inputSchema.getField(inputField) == null) {
          failureCollector.addFailure(
            String.format("Input field '%s' does not exist in the input schema.", inputField), null)
            .withConfigProperty(FIELDS_TO_PROCESS)
            .withInputSchemaField(inputField);
        }
      }
    }
  }

  /**
   * Validates whether the output field type is supported or not.
   */
  private void validateOutputFieldType(FailureCollector failureCollector) {
    switch (outputFieldType) {
      case "boolean":
      case "bytes":
      case "double":
      case "float":
      case "int":
      case "long":
      case "string":
        break;
      default:
        failureCollector.addFailure(
          String.format("Schema type '%s' for output field is not supported.", outputFieldType),
          "Supported types are: 'boolean, bytes, double, float, int, long and string'.")
          .withConfigProperty(OUTPUT_FIELD_TYPE);
    }
  }

  /**
   * Verifies whether the type of output field coming through schema is proper or not.
   */
  private void verifyOutputFieldTypeInSchema(FailureCollector failureCollector) {
    try {
      Schema outputSchema = Schema.parseJson(schema);
      List<Schema.Field> outputFields = outputSchema.getFields();
      for (Schema.Field field : outputFields) {
        if (field.getName().equals(outputField) && !field.getSchema().isNullable()) {
          failureCollector.addFailure(
            String.format("Output Field '%s' should be of nullable type.", outputField),
            "Provide correct output schema.")
            .withConfigProperty(OUTPUT_FIELD);
        }
      }

      for (Schema.Field field : outputFields) {
        if (field.getName().equals(outputField)) {
          String type = field.getSchema().isNullable()
            ? field.getSchema().getNonNullable().getDisplayName()
            : field.getSchema().getDisplayName();
          if (!type.equals(outputFieldType)) {
            failureCollector.addFailure(
              String.format("Type mismatch for the Output Field '%s'.", outputField),
              String.format("Type should be '%s' but found '%s'. Provide correct output schema.",
                            Schema.of(Schema.Type.valueOf(outputFieldType)).getDisplayName(), type))
              .withConfigProperty(OUTPUT_FIELD);
          }
        }
      }
    } catch (IOException e) {
      failureCollector.addFailure("Unable to parse the output schema.", null)
      .withStacktrace(e.getStackTrace());
    }
  }

  /**
   * Builder for RunConfig
   */
  public static final class Builder {
    private String commandToExecute;
    private String fieldsToProcess;
    private String fixedInputs;
    private String outputField;
    private String outputFieldType;
    private String schema;

    private Builder() {
    }

    public Builder setCommandToExecute(String commandToExecute) {
      this.commandToExecute = commandToExecute;
      return this;
    }

    public Builder setFieldsToProcess(String fieldsToProcess) {
      this.fieldsToProcess = fieldsToProcess;
      return this;
    }

    public Builder setFixedInputs(String fixedInputs) {
      this.fixedInputs = fixedInputs;
      return this;
    }

    public Builder setOutputField(String outputField) {
      this.outputField = outputField;
      return this;
    }

    public Builder setOutputFieldType(String outputFieldType) {
      this.outputFieldType = outputFieldType;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public RunConfig build() {
      return new RunConfig(this);
    }
  }
}
