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

package org.apache.rocketmq.client.consumer;

import java.util.Properties;
import org.apache.rocketmq.client.consumer.listener.SchemaMessageListenerConcurrently;

public class SchemaMQPushConsumer<T> extends DefaultMQPushConsumer {

    public static Properties properties;

    /**
     * Default constructor.
     */
    public SchemaMQPushConsumer(final Properties properties) {
        super(properties.getProperty("consumer.group", "default_cg"));
        setNamesrvAddr(properties.getProperty("namesrv", "localhost:9876"));
        SchemaMQPushConsumer.properties = properties;
    }

    public void registerMessageListener(SchemaMessageListenerConcurrently<T> messageListener) {
        setMessageListener(messageListener);
        this.defaultMQPushConsumerImpl.registerMessageListener(messageListener);
    }
}
