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

import com.google.gson.Gson;
import java.io.IOException;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.rocketmq.schema.registry.common.model.SchemaInfo;

public class SerdeUtil<T> {

    // TODO init url / path variable
    public SchemaInfo getSchemaInfo(String url, String path, String topic) {
        Gson gson = new Gson();
        String result = doGet(url + path + topic);
        return gson.fromJson(result, SchemaInfo.class) ;
    }

    public String doGet(String url) {
        CloseableHttpResponse response = null;
        String result = "";
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpGet httpGet = new HttpGet(url);
            httpGet.setHeader("Authorization", "");
            RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(3000)
                .setConnectionRequestTimeout(5000)
                .setSocketTimeout(5000)
                .build();
            httpGet.setConfig(requestConfig);
            response = httpClient.execute(httpGet);
            HttpEntity entity = response.getEntity();
            result = EntityUtils.toString(entity);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

}
