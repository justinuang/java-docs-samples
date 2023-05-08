/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// [START bigtable_beam_helloworld_read]
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.hbase.BigtableOptionsFactory;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HelloWorldRead {

  private static final Logger LOG = LoggerFactory.getLogger(HelloWorldRead.class);
  public static void main(String[] args) {
    BigtableOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);
    options.setMaxNumWorkers(3);
    Pipeline p = Pipeline.create(options);

    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    scan.setFilter(new FirstKeyOnlyFilter());

    CloudBigtableScanConfiguration config =
        new CloudBigtableScanConfiguration.Builder()
            .withProjectId(options.getBigtableProjectId())
            .withInstanceId(options.getBigtableInstanceId())
            .withTableId(options.getBigtableTableId())
            .withAppProfileId("offline")
            .withScan(scan)
            .withConfiguration(BigtableOptionsFactory.BIGTABLE_HOST_KEY,
                "test-bigtable.sandbox.googleapis.com")
            .withConfiguration(BigtableOptionsFactory.BIGTABLE_USE_BATCH, "false")
            .build();

    // [START bigtable_beam_helloworld_read_transforms]
    p.apply(Read.from(CloudBigtableIO.read(config)))
        .apply(Count.globally())
        .apply(
            ParDo.of(
                new DoFn<Long, Void>() {
                  @ProcessElement
                  public void processElement(@Element Long count, OutputReceiver<Void> out) {
                    LOG.info("Total Count: " + count);
                  }
                }));
    // [END bigtable_beam_helloworld_read_transforms]

    p.run().waitUntilFinish();
  }

  public interface BigtableOptions extends DataflowPipelineOptions {
    @Description("The Bigtable project ID, this can be different than your Dataflow project")
    @Default.String("google.com:cloud-bigtable-dev")
    String getBigtableProjectId();

    void setBigtableProjectId(String bigtableProjectId);

    @Description("The Bigtable instance ID")
    @Default.String("justinuang-debug-mem")
    String getBigtableInstanceId();

    void setBigtableInstanceId(String bigtableInstanceId);

    @Description("The Bigtable table ID in the instance.")
    @Default.String("manytabletstable")
    String getBigtableTableId();

    void setBigtableTableId(String bigtableTableId);
  }
}
// [END bigtable_beam_helloworld_read]

