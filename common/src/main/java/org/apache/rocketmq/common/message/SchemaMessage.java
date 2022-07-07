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

package org.apache.rocketmq.common.message;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;

public class SchemaMessage<T> implements Serializable {
    private static final long serialVersionUID = 5258253154937433300L;

    private Message message;
    private T originMessage;
    private final AtomicBoolean isEncoded = new AtomicBoolean(false);

    public SchemaMessage() {
    }

    public SchemaMessage(String topic, T originMessage) {
        this.originMessage = originMessage;
        this.message = new Message(topic,  "", "", 0, null, true);
    }

    public SchemaMessage(String topic, String tags, T originMessage) {
        this.originMessage = originMessage;
        this.message = new Message(topic, tags, "", 0, null, true);
    }

    public SchemaMessage(String topic, String tags, String keys, T originMessage) {
        this.originMessage = originMessage;
        this.message = new Message(topic, tags, keys, 0, null, true);
    }

    public Message encode() {
        return this.message;
    }

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
    }

    public T getOriginMessage() {
        return originMessage;
    }

    public boolean isEncoded() {
        return isEncoded.get();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"message\":")
            .append(message);
        sb.append(",\"originMessage\":")
            .append(originMessage);
        sb.append(",\"isEncoded\":")
            .append(isEncoded);
        sb.append('}');
        return sb.toString();
    }
}
