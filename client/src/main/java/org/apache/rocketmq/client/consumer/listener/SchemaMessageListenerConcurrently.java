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

package org.apache.rocketmq.client.consumer.listener;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.rocketmq.client.consumer.SchemaMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.SchemaMessageExt;
import org.apache.rocketmq.common.utils.SchemaEntity;
import org.apache.rocketmq.common.utils.SerdeUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;

public abstract class SchemaMessageListenerConcurrently<T> implements MessageListenerConcurrently {
    private final InternalLogger log = ClientLogger.getLog();

    private SerdeUtil<T> serdeUtil = new SerdeUtil<>();
    private static Map<String, SchemaEntity> topicSchemas = new HashMap<String, SchemaEntity>();

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
        final ConsumeConcurrentlyContext context) {
        List<SchemaMessageExt<T>> schemaMsgs = msgs.stream()
            .map(msg -> {
                String topic = msg.getTopic();
                // TODO init topic schema wrapper
                if (topicSchemas.get(topic) == null) {
                    synchronized (this) {
                        if (topicSchemas.get(topic) == null) {
                            SchemaInfo schemaInfo = serdeUtil.getSchemaInfo(SchemaMQPushConsumer.properties.getProperty("schema.registry.url"), SchemaMQPushConsumer.properties.getProperty("schema.registry.get.schema.path"), topic);
                            try {
                                Schema schema = new Schema.Parser().parse(schemaInfo.getDetails().lastRecord().getIdl());
                                String schemaName = schemaInfo.getMeta().getSchemaName();
                                long schemaId = schemaInfo.getMeta().getUniqueId();
                                SchemaEntity entity = new SchemaEntity(schemaId, schemaName, schema);
                                topicSchemas.put(topic, entity);
                            } catch (Exception e) {
                                log.error("get schema failed", e);
                                throw new RuntimeException("get schema failed", e);
                            }
                        }
                    }
                }

                T schemaMessage = null;
                try {
                    schemaMessage = avroDecode(msg.getBody(), topicSchemas.get(topic).getSchema());
                } catch (Exception e) {
                    log.error("decode message failed", e);
                    throw new RuntimeException("decode message failed", e);
                }
                return new SchemaMessageExt<T>(msg, schemaMessage);
            })
            .collect(Collectors.toList());
        return consumeSchemaMessage(schemaMsgs, context);
    }


    public T avroDecode(byte[] input, Schema schema) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(input);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bais, null);

        ByteBuffer buffer = ByteBuffer.allocate(16);
        try {
            decoder.readBytes(buffer);
        } catch (Exception e) {
            log.error("read byets error: ", e);
        }

        long id = buffer.getLong();
        // todo check schema <-> id valid

        DatumReader<T> datumReader = new SpecificDatumReader<T>(schema);
        T originMessage = datumReader.read(null, decoder);
        return originMessage;
    }

    public abstract ConsumeConcurrentlyStatus consumeSchemaMessage(final List<SchemaMessageExt<T>> msgs,
                                                                   final ConsumeConcurrentlyContext context);
}
