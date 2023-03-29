package org.geovistory.kafka.sink.connector.rdf.sender;

import org.geovistory.kafka.sink.connector.rdf.config.HttpSinkConfig;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class AccessTokenHttpRequestBuilderTest {

    @Test
    void shouldThrowExceptionWithoutConfig() {
        final Exception thrown = assertThrows(NullPointerException.class, () ->
                new AccessTokenHttpRequestBuilder().build(null).build()
        );
        assertEquals("config should not be null", thrown.getMessage());
    }

    @Test
    void shouldThrowExceptionWithoutRightConfig() {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "apikey",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);

        final Exception thrown = assertThrows(IllegalArgumentException.class, () ->
                new AccessTokenHttpRequestBuilder().build(config).build()
        );
        assertEquals("The expected authorization type is oauth2", thrown.getMessage());
    }

    @Test
    void shouldBuildDefaultAccessTokenRequest() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var accessTokenRequest =
                new AccessTokenHttpRequestBuilder().build(config).build();

        assertThat(accessTokenRequest.uri()).isEqualTo(new URL("http://localhost:42/token").toURI());

        final var expectedAuthHeader = "Basic "
                + Base64.getEncoder()
                    .encodeToString("some_client_id:some_client_secret".getBytes(StandardCharsets.UTF_8));

        assertThat(accessTokenRequest.timeout()).isPresent()
                .get(as(InstanceOfAssertFactories.DURATION))
                .hasSeconds(config.httpTimeout());
        assertThat(accessTokenRequest.method()).isEqualTo("POST");
        assertThat(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE))
                .hasValue("application/x-www-form-urlencoded");
        assertThat(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION))
                .hasValue(expectedAuthHeader);

    }

    @Test
    void shouldBuildCustomisedAccessTokenRequest() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret",
                "oauth2.client.authorization.mode", "url",
                "oauth2.client.scope", "scope1,scope2"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var accessTokenRequest =
                new AccessTokenHttpRequestBuilder().build(config).build();

        assertThat(accessTokenRequest.uri()).isEqualTo(new URL("http://localhost:42/token").toURI());

        assertThat(accessTokenRequest.timeout()).isPresent()
                .get(as(InstanceOfAssertFactories.DURATION))
                .hasSeconds(config.httpTimeout());

        assertThat(accessTokenRequest.method()).isEqualTo("POST");
        assertThat(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE))
                .hasValue("application/x-www-form-urlencoded");
        assertThat(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_AUTHORIZATION)).isEmpty();
    }

}
