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

package org.apache.rocketmq.common.utils;

import org.apache.avro.Schema;

public class SchemaEntity {
    long schemaId;
    String schemaName;
    Schema schema;

    public SchemaEntity(long schemaId, String schemaName, Schema schema) {
        this.schemaId = schemaId;
        this.schemaName = schemaName;
        this.schema = schema;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public void setSchemaId(long schemaId) {
        this.schemaId = schemaId;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("{");
        sb.append("\"schemaId\":")
            .append(schemaId);
        sb.append(",\"schemaName\":\"")
            .append(schemaName).append('\"');
        sb.append(",\"schema\":")
            .append(schema);
        sb.append('}');
        return sb.toString();
    }
}
