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

package org.apache.pulsar.io.kafka;

import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.KeyValue;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.pulsar.io.kafka.connect.ProducerRecordWithSchema;

import java.util.Properties;

/**
 * Kafka sink should treats incoming messages as pure bytes. So we don't
 * apply schema into it.
 */
@Connector(
        name = "kafka",
        type = IOType.SINK,
        help = "The KafkaBytesSink is used for moving messages from Pulsar to Kafka.",
        configClass = KafkaSinkConfig.class
)
@Slf4j
public class KafkaObjectsSink extends KafkaAbstractSink<Object, Object> {

    // todo: to config
    private boolean unwrapKeyValueIfAvailable = true;

    private final static ImmutableMap<Class, Schema> primitiveTypeToSchema;
    static {
        primitiveTypeToSchema = ImmutableMap.<Class, Schema>builder()
                .put(Boolean.class, Schema.BOOLEAN_SCHEMA)
                .put(Byte.class, Schema.INT8_SCHEMA)
                .put(Short.class, Schema.INT16_SCHEMA)
                .put(Integer.class, Schema.INT32_SCHEMA)
                .put(Long.class, Schema.INT64_SCHEMA)
                .put(Float.class, Schema.FLOAT32_SCHEMA)
                .put(Double.class, Schema.FLOAT32_SCHEMA)
                .put(String.class, Schema.STRING_SCHEMA)
                .put(byte[].class, Schema.BYTES_SCHEMA)
                .build();
    }

    @Override
    protected Properties beforeCreateProducer(Properties props) {
        // get these from config for actual kafka producer
        // KafkaSinkWrappingProducer does not use this, KafkaSinks will have their own config
        // todo:?? props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // todo:?? props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        log.info("Created kafka producer config : {}", props);
        return props;
    }

    @Override
    public ProducerRecord<Object, Object> toProducerRecord(Record<Object> record) {
        Object key;
        Object value = record.getValue();
        if (unwrapKeyValueIfAvailable && value instanceof KeyValue) {
            KeyValue<Object, Object> kv = (KeyValue<Object, Object>) value;
            key = kv.getKey();
            value = kv.getValue();
        } else {
            key = record.getKey().orElse(null);
        }

        Schema keySchema = getSchemaForObject(key);
        Schema valueSchema = getSchemaForObject(value);

        return new ProducerRecordWithSchema<>(topicName, key, value, keySchema, valueSchema);
    }

    public static Schema getSchemaForObject(Object obj) {
        if (obj == null) {
            return null;
        }

        if (primitiveTypeToSchema.containsKey(obj.getClass())) {
            return primitiveTypeToSchema.get(obj.getClass());
        }

        // other types not supported yet
        return null;
    }
}