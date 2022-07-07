/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.producer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.SchemaMessage;
import org.apache.rocketmq.common.utils.SchemaEntity;
import org.apache.rocketmq.common.utils.SerdeUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;

public class SchemaMQProducer<T> extends DefaultMQProducer {
    private final InternalLogger log = ClientLogger.getLog();

    private Map<String, SchemaEntity> topicSchemas;
    private SerdeUtil<T> serdeUtil;
    private static Properties producerProps;

    public SchemaMQProducer(final Properties properties) {
        super(properties.getProperty("producer.group", "default_pg"));
        setNamesrvAddr(properties.getProperty("namesrv", "localhost:9876"));

        producerProps = properties;
        topicSchemas = new HashMap<String, SchemaEntity>();
        serdeUtil = new SerdeUtil<T>();
    }

    public SendResult send(SchemaMessage<T> msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        String topic = msg.getMessage().getTopic();

        if (topicSchemas.get(topic) == null) {
            SchemaInfo schemaInfo = serdeUtil.getSchemaInfo(producerProps.getProperty("schema.registry.url"),
                producerProps.getProperty("schema.registry.get.schema.path"), topic);
            try {
                Schema schema = new Schema.Parser().parse(schemaInfo.getDetails().lastRecord().getIdl());
                String schemaName = schemaInfo.getMeta().getSchemaName();
                long schemaId = schemaInfo.getMeta().getUniqueId();
                SchemaEntity entity = new SchemaEntity(schemaId, schemaName, schema);
                topicSchemas.put(topic, entity);
            } catch (Exception e) {
                log.error("get schema failed", e);
            }
        }

        SchemaEntity entity = topicSchemas.get(topic);

        byte[] encodedMsg = null;
        try {
            encodedMsg = avroEncode(msg.getOriginMessage(), entity.getSchemaId(), entity.getSchema());
        } catch (IOException e) {
            throw new MQClientException("Serialized message failed", e);
        }
        msg.getMessage().setBody(encodedMsg);
        return send(msg.getMessage());
    }

    public byte[] avroEncode(T originMessage, long schemaId, Schema schema) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        BinaryEncoder encoder = null;
        encoder = EncoderFactory.get().binaryEncoder(baos, encoder);

        // header
        encoder.writeBytes(ByteBuffer.allocate(16).putLong(schemaId).array());

        DatumWriter<T> datumWriter = new SpecificDatumWriter<T>(schema);
        datumWriter.write(originMessage, encoder);
        encoder.flush();
        return baos.toByteArray();
    }

}
