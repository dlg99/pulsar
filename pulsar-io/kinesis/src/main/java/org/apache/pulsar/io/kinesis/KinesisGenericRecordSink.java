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

package org.apache.pulsar.io.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.producer.KinesisProducer;

import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.aws.AwsCredentialProviderPlugin;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;

import java.nio.ByteBuffer;

/**
 * A Kinesis sink which can be configured by {@link KinesisSinkConfig}.
 * <pre>
 * {@link KinesisSinkConfig} accepts
 * 1. <b>awsEndpoint:</b> kinesis end-point url can be found at : https://docs.aws.amazon.com/general/latest/gr/rande.html
 * 2. <b>awsRegion:</b> appropriate aws region eg: us-west-1, us-west-2
 * 3. <b>awsKinesisStreamName:</b> kinesis stream name
 * 4. <b>awsCredentialPluginName:</b> Fully-Qualified class name of implementation of {@link AwsCredentialProviderPlugin}.
 *    - It is a factory class which creates an {@link AWSCredentialsProvider} that will be used by {@link KinesisProducer}
 *    - If it is empty then {@link KinesisSink} creates default {@link AWSCredentialsProvider}
 *      which accepts json-map of credentials in awsCredentialPluginParam
 *      eg: awsCredentialPluginParam = {"accessKey":"my-access-key","secretKey":"my-secret-key"}
 * 5. <b>awsCredentialPluginParam:</b> json-parameters to initialize {@link AwsCredentialProviderPlugin}
 * </pre>
 *
 * todo: update
 *
 */
@Connector(
        name = "kinesis",
        type = IOType.SINK,
        help = "A sink connector that copies messages from Pulsar to Kinesis",
        configClass = KinesisSinkConfig.class
)
public class KinesisGenericRecordSink extends AbstractKinesisSink<GenericRecord> {

    private GlueSchemaRegistrySerializerImpl glueSchemaRegistrySerializer;

    @Override
    protected void postKinesisConfig(KinesisProducerConfiguration kinesisConfig, SinkContext sinkContext) {
        // Schema Registry integration is only available with KPL v0.14.2 or later and with KCL v2.3 or later.
        GlueSchemaRegistryConfiguration schemaRegistryConfig =
                new GlueSchemaRegistryConfiguration(kinesisConfig.getRegion());
        // todo: other settings, see https://github.com/awslabs/aws-glue-schema-registry/blob/master/README.md
        kinesisConfig.setGlueSchemaRegistryConfiguration(schemaRegistryConfig);

        software.amazon.awssdk.auth.credentials.AwsCredentialsProvider authProvider = createCredentialProvider(
                        kinesisSinkConfig.getAwsCredentialPluginName(),
                        kinesisSinkConfig.getAwsCredentialPluginParam())
                .getV2CredentialsProvider();
        glueSchemaRegistrySerializer = new GlueSchemaRegistrySerializerImpl(authProvider, kinesisConfig.getRegion());
    }

    @Override
    public com.amazonaws.services.schemaregistry.common.Schema getAwsSchema(Record<GenericRecord> record) {
        Schema<GenericRecord> pulsarSchema = record.getSchema();
        if (null == pulsarSchema) {
            return null;
        }

        com.amazonaws.services.schemaregistry.common.Schema awsSchema =
                new com.amazonaws.services.schemaregistry.common.Schema(
                        pulsarSchema.getSchemaInfo().getSchemaDefinition(), /* definition */
                        pulsarSchema.getSchemaInfo().getType().toString(), /* data format */
                        pulsarSchema.getSchemaInfo().getName()); /* name */
        return awsSchema;
    }

    @Override
    public ByteBuffer createKinesisMessage(Record<GenericRecord> record) {
        Schema<GenericRecord> pulsarSchema = record.getSchema();
        if (null == pulsarSchema) {
            Object value = record.getValue();
            if (value instanceof byte[]) {
                return ByteBuffer.wrap((byte[])value);
            }
            throw new IllegalArgumentException("Cannot process schema-less GenericRecord of type "
                    + value.getClass().getCanonicalName());
        }

        byte[] recordAsBytes = pulsarSchema.encode(record.getValue());
        return ByteBuffer.wrap(recordAsBytes);
    }

}