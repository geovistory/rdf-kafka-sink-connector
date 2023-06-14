/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
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

import org.apache.kafka.connect.errors.ConnectException;
import org.geovistory.kafka.sink.connector.rdf.config.HttpSinkConfig;
import org.geovistory.kafka.sink.connector.rdf.fuseki.DatasetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.TimeUnit;


public class HttpSender {

    private static final Logger log = LoggerFactory.getLogger(HttpSender.class);

    protected final HttpClient httpClient;

    protected final HttpSinkConfig config;

    public HttpRequestBuilder httpRequestBuilder;

    protected HttpSender(final HttpSinkConfig config) {
        this(config, HttpRequestBuilder.DEFAULT_HTTP_REQUEST_BUILDER);
    }

    protected HttpSender(final HttpSinkConfig config,
                         final HttpRequestBuilder httpRequestBuilder) {
        this(config, httpRequestBuilder, HttpClient.newHttpClient());
    }

    protected HttpSender(final HttpSinkConfig config,
                         final HttpClient httpClient) {
        this(config, HttpRequestBuilder.DEFAULT_HTTP_REQUEST_BUILDER, httpClient);
    }

    protected HttpSender(final HttpSinkConfig config,
                         final HttpRequestBuilder httpRequestBuilder,
                         final HttpClient httpClient) {
        this.config = config;
        this.httpRequestBuilder = httpRequestBuilder;
        this.httpClient = httpClient;
    }

    public final void send(final String body, String projectId) {
        final var requestBuilder =
                httpRequestBuilder
                        .build(config, projectId)
                        .POST(HttpRequest.BodyPublishers.ofString(body));

        sendWithRetries(requestBuilder, HttpResponseHandler.ON_HTTP_ERROR_RESPONSE_HANDLER, projectId);
    }

    /**
     * Sends a HTTP body using {@code httpSender}, respecting the configured retry policy.
     *
     * @return whether the sending was successful.
     */
    protected HttpResponse<String> sendWithRetries(final HttpRequest.Builder requestBuilder,
                                                   final HttpResponseHandler httpResponseHandler,
                                                   final String projectId) {
        //TODO check if dataset exists here
        int remainRetries = config.maxRetries();
        while (remainRetries >= 0) {
            try {
                try {
                    final var response =
                            httpClient.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
                    log.debug("Server replied with status code {} and body {}", response.statusCode(), response.body());
                    var code = httpResponseHandler.onResponse(response);
                    if (code == ResponseSenderCode.DATASET_NOT_EXISTING) {
                        DatasetHandler.createFusekiDataset(config.getDatasetName(projectId), config.getHttpUrlConfig(), config.getHttpHeadersAuthorizationConfig());
                        remainRetries++;
                        throw new Exception("Dataset was not existing. Created dataset.");
                    }
                    if (!code.equals(200)) {
                        throw new Exception("Error sending record " + response);
                    }
                    return response;
                } catch (final Exception e) {
                    log.info("Sending failed, will retry in {} ms ({} retries remain)",
                            config.retryBackoffMs(), remainRetries, e);
                    remainRetries -= 1;
                    TimeUnit.MILLISECONDS.sleep(config.retryBackoffMs());
                }
            } catch (final InterruptedException e) {
                log.error("Sending failed due to InterruptedException, stopping", e);
                throw new ConnectException(e);
            }
        }
        log.error("Sending failed and no retries remain, stopping");
        throw new ConnectException("Sending failed and no retries remain, stopping");
    }

    public static HttpSender createHttpSender(final HttpSinkConfig config) {
        switch (config.authorizationType()) {
            case NONE:
                return new HttpSender(config);
            case STATIC:
                return new HttpSender(config, HttpRequestBuilder.AUTH_HTTP_REQUEST_BUILDER);
            case OAUTH2:
                return new OAuth2HttpSender(config, new AccessTokenHttpRequestBuilder());
            case APIKEY:
                return new OAuth2HttpSender(config, new ApiKeyAccessTokenHttpRequestBuilder());
            default:
                throw new ConnectException("Can't create HTTP sender for auth type: " + config.authorizationType());
        }
    }

}
