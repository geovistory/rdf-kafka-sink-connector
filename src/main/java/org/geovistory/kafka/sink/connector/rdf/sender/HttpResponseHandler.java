/*
 * Copyright 2021 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package org.geovistory.kafka.sink.connector.rdf.sender;

import org.geovistory.kafka.sink.connector.rdf.fuseki.DatasetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.http.HttpResponse;

interface HttpResponseHandler {

    Logger LOGGER = LoggerFactory.getLogger(HttpResponseHandler.class);

    ResponseSenderCode onResponse(final HttpResponse<String> response) throws IOException;

    HttpResponseHandler ON_HTTP_ERROR_RESPONSE_HANDLER = response -> {
        final var request = response.request();
        final var uri = request != null ? request.uri() : "UNKNOWN";
        if (response.statusCode() == 404) {
           return ResponseSenderCode.DATASET_NOT_EXISTING;
        }
        if (response.statusCode() >= 400) {

            LOGGER.warn(
                    "Got unexpected HTTP status code: {} and body: {}. Requested URI: {}",
                    response.statusCode(),
                    response.body(),
                    uri);
            throw new IOException("Server replied with status code " + response.statusCode()
                    + " and body " + response.body());
        }
        return null;
    };
}
