/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.kafka.connect;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.file.FileStreamSourceConnector;
import org.apache.kafka.connect.runtime.TaskConfig;
import org.apache.pulsar.client.api.ProducerConsumerBase;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Test the implementation of {@link KafkaConnectSource}.
 */
@Slf4j
public class KafkaConnectSourceErrRecTest extends ProducerConsumerBase  {

    private Map<String, Object> config = new HashMap<>();
    private String offsetTopicName;
    // The topic to publish data to, for kafkaSource
    private String topicName;
    private KafkaConnectSource kafkaConnectSource;
    private File tempFile;
    private SourceContext context;
    private PulsarClient client;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        super.producerBaseSetup();

        config.put(TaskConfig.TASK_CLASS_CONFIG, "org.apache.pulsar.io.kafka.connect.ErrRecFileStreamSourceTask");
        config.put(PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");
        config.put(PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, "org.apache.kafka.connect.storage.StringConverter");

        this.offsetTopicName = "persistent://my-property/my-ns/kafka-connect-source-offset";
        config.put(PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG, offsetTopicName);

        this.topicName = "persistent://my-property/my-ns/kafka-connect-source";
        config.put(FileStreamSourceConnector.TOPIC_CONFIG, topicName);
        tempFile = File.createTempFile("some-file-name", null);
        config.put(FileStreamSourceConnector.FILE_CONFIG, tempFile.getAbsoluteFile().toString());
        config.put(FileStreamSourceConnector.TASK_BATCH_SIZE_CONFIG, String.valueOf(FileStreamSourceConnector.DEFAULT_TASK_BATCH_SIZE));

        this.context = mock(SourceContext.class);
        this.client = PulsarClient.builder()
                .serviceUrl(brokerUrl.toString())
                .build();
        when(context.getPulsarClient()).thenReturn(this.client);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (this.client != null) {
            this.client.close();
        }
        tempFile.delete();
        super.internalCleanup();
    }

    @Test
    public void testCommitRecordCalled() throws Exception {
        kafkaConnectSource = new KafkaConnectSource();
        kafkaConnectSource.open(config, context);

        // use FileStreamSourceConnector, each line is a record, need "\n" and end of each record.
        OutputStream os = Files.newOutputStream(tempFile.toPath());

        String line1 = "This is the first line\n";
        os.write(line1.getBytes());
        os.flush();
        os.close();

        Record<KeyValue<byte[], byte[]>> record = kafkaConnectSource.read();

        assertTrue(record instanceof KafkaConnectSource.KafkaSourceRecord);

        try {
            record.ack();
            fail("expected exception");
        } catch (Exception e) {
            log.info("got exception", e);
            assertTrue(e instanceof org.apache.kafka.connect.errors.ConnectException);
        }
    }
}
