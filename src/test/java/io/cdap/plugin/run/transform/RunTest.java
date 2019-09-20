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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.api.artifact.ArtifactSummary;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.table.Table;
import io.cdap.cdap.datapipeline.DataPipelineApp;
import io.cdap.cdap.datapipeline.SmartWorkflow;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.batch.MockSink;
import io.cdap.cdap.etl.mock.batch.MockSource;
import io.cdap.cdap.etl.mock.test.HydratorTestBase;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.ApplicationManager;
import io.cdap.cdap.test.DataSetManager;
import io.cdap.cdap.test.TestConfiguration;
import io.cdap.cdap.test.WorkflowManager;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test Cases for Run.
 */
public class RunTest extends HydratorTestBase {
  private static final Schema INPUT = Schema.recordOf("input", Schema.Field.of("id", Schema.of(Schema.Type.INT)),
                                                      Schema.Field.of("input", Schema.of(Schema.Type.STRING)));

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);
  private static final ArtifactVersion CURRENT_VERSION = new ArtifactVersion("6.1.0");
  private static final ArtifactId BATCH_APP_ARTIFACT_ID =
    NamespaceId.DEFAULT.artifact("data-pipeline", CURRENT_VERSION.getVersion());
  protected static final ArtifactSummary BATCH_ARTIFACT =
    new ArtifactSummary(BATCH_APP_ARTIFACT_ID.getArtifact(), BATCH_APP_ARTIFACT_ID.getVersion());

  @ClassRule
  public static TemporaryFolder temporaryFolder = new TemporaryFolder();
  @ClassRule
  public static TemporaryFolder folder = new TemporaryFolder();
  private static File sourceFolder;

  @BeforeClass
  public static void setupTest() throws Exception {
    // Add the ETL batch artifact and mock plugins.
    setupBatchArtifacts(BATCH_APP_ARTIFACT_ID, DataPipelineApp.class);

    // Add our plugins artifact with the ETL batch artifact as its parent.
    // This will make our plugins available to the ETL batch.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("run-transform", "1.0.0"), BATCH_APP_ARTIFACT_ID,
                      Base64.class, Base32.class, Run.class);

    sourceFolder = temporaryFolder.newFolder("run");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    folder.delete();
    temporaryFolder.delete();
  }

  @Test
  public void testRunWithJarInput() throws Exception {
    String inputTable = "run-jar-input";
    URL testRunnerUrl = this.getClass().getResource("/SampleRunner.jar");
    FileUtils.copyFile(new File(testRunnerUrl.getFile()), new File(sourceFolder, "/SampleRunner.jar"));

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable, INPUT));

    Map<String, String> runProperties = new ImmutableMap.Builder<String, String>()
      .put("commandToExecute", "java -jar " + sourceFolder.toPath() + "/SampleRunner.jar")
      .put("fieldsToProcess", "input")
      .put("fixedInputs", "CASK")
      .put("outputField", "output")
      .put("outputFieldType", "string")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("Run", Transform.PLUGIN_TYPE, runProperties, null));

    String sinkTable = "run-jar-output";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("RunJarTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT).set("id", 1).set("input", "Brett").build(),
      StructuredRecord.builder(INPUT).set("id", 2).set("input", "Chang").build(),
      StructuredRecord.builder(INPUT).set("id", 3).set("input", "Roy").build(),
      StructuredRecord.builder(INPUT).set("id", 4).set("input", "John").build(),
      StructuredRecord.builder(INPUT).set("id", 5).set("input", "Michael").build());

    MockSource.writeInput(inputManager, input);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals("OutputRecords", 5, outputRecords.size());
    for (StructuredRecord record : outputRecords) {
      int value = (record.get("id"));
      if (value == 1) {
        Assert.assertEquals("Brett", record.get("input"));
        Assert.assertEquals("Hello Brett...Welcome to the CASK!!!", record.get("output"));
      } else if (value == 2) {
        Assert.assertEquals("Chang", record.get("input"));
        Assert.assertEquals("Hello Chang...Welcome to the CASK!!!", record.get("output"));
      } else if (value == 3) {
        Assert.assertEquals("Roy", record.get("input"));
        Assert.assertEquals("Hello Roy...Welcome to the CASK!!!", record.get("output"));
      } else if (value == 4) {
        Assert.assertEquals("John", record.get("input"));
        Assert.assertEquals("Hello John...Welcome to the CASK!!!", record.get("output"));
      } else {
        Assert.assertEquals("Michael", record.get("input"));
        Assert.assertEquals("Hello Michael...Welcome to the CASK!!!", record.get("output"));
      }
    }
  }

  @Test
  public void testRunWithScriptInput() throws Exception {
    String inputTable = "run-shell-script-input";
    URL testRunnerUrl = this.getClass().getResource("/SampleScript.sh");
    FileUtils.copyFile(new File(testRunnerUrl.getFile()), new File(sourceFolder, "/SampleScript.sh"));

    ETLStage source = new ETLStage("source", MockSource.getPlugin(inputTable, INPUT));

    Map<String, String> runProperties = new ImmutableMap.Builder<String, String>()
      .put("commandToExecute", "sh " + sourceFolder.toPath() + "/SampleScript.sh")
      .put("fieldsToProcess", "input")
      .put("outputField", "output")
      .put("outputFieldType", "string")
      .build();

    ETLStage transform = new ETLStage("transform",
                                      new ETLPlugin("Run", Transform.PLUGIN_TYPE, runProperties, null));

    String sinkTable = "run-shell-script-output";

    ETLStage sink = new ETLStage("sink", MockSink.getPlugin(sinkTable));
    ETLBatchConfig etlConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(source)
      .addStage(transform)
      .addStage(sink)
      .addConnection(source.getName(), transform.getName())
      .addConnection(transform.getName(), sink.getName())
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(BATCH_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("RunScriptTest");
    ApplicationManager appManager = deployApplication(appId, appRequest);

    DataSetManager<Table> inputManager = getDataset(inputTable);
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(INPUT).set("id", 1).set("input", "Brett").build(),
      StructuredRecord.builder(INPUT).set("id", 2).set("input", "Chang").build(),
      StructuredRecord.builder(INPUT).set("id", 3).set("input", "Roy").build(),
      StructuredRecord.builder(INPUT).set("id", 4).set("input", "John").build(),
      StructuredRecord.builder(INPUT).set("id", 5).set("input", "Matt").build());

    MockSource.writeInput(inputManager, input);
    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.startAndWaitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    DataSetManager<Table> outputManager = getDataset(sinkTable);
    List<StructuredRecord> outputRecords = MockSink.readOutput(outputManager);

    Assert.assertEquals("OutputRecords", 5, outputRecords.size());
    for (StructuredRecord record : outputRecords) {
      int value = (record.get("id"));
      if (value == 1) {
        Assert.assertEquals("Brett", record.get("input"));
        Assert.assertEquals("Welcome User, Brett.", record.get("output"));
      } else if (value == 2) {
        Assert.assertEquals("Chang", record.get("input"));
        Assert.assertEquals("Welcome User, Chang.", record.get("output"));
      } else if (value == 3) {
        Assert.assertEquals("Roy", record.get("input"));
        Assert.assertEquals("Welcome User, Roy.", record.get("output"));
      } else if (value == 4) {
        Assert.assertEquals("John", record.get("input"));
        Assert.assertEquals("Welcome User, John.", record.get("output"));
      } else {
        Assert.assertEquals("Matt", record.get("input"));
        Assert.assertEquals("Welcome User, Matt.", record.get("output"));
      }
    }
  }
}
