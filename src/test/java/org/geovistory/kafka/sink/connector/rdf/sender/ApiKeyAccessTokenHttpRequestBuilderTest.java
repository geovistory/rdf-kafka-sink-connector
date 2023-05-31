package org.geovistory.kafka.sink.connector.rdf.sender;

import org.geovistory.kafka.sink.connector.rdf.config.HttpSinkConfig;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.Map;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ApiKeyAccessTokenHttpRequestBuilderTest {

    @Test
    void shouldBuildDefaultAccessTokenRequest() throws Exception {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "apikey",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);
        final var accessTokenRequest =
                new ApiKeyAccessTokenHttpRequestBuilder().build(config, null).build();

        assertThat(accessTokenRequest.uri()).isEqualTo(new URL("http://localhost:42/token").toURI());

        assertThat(accessTokenRequest.timeout()).isPresent()
                .get(as(InstanceOfAssertFactories.DURATION))
                .hasSeconds(config.httpTimeout());
        assertThat(accessTokenRequest.method()).isEqualTo("POST");
        assertThat(accessTokenRequest.headers().firstValue(HttpRequestBuilder.HEADER_CONTENT_TYPE))
                .hasValue("application/x-www-form-urlencoded");

    }

    @Test
    void shouldThrowExceptionWithoutConfig() {
        final Exception thrown = assertThrows(NullPointerException.class, () ->
            new ApiKeyAccessTokenHttpRequestBuilder().build(null, null).build()
        );
        assertEquals("config should not be null", thrown.getMessage());
    }

    @Test
    void shouldThrowExceptionWithoutRightConfig() {
        final var configBase = Map.of(
                "http.url", "http://localhost:42",
                "http.authorization.type", "oauth2",
                "oauth2.access.token.url", "http://localhost:42/token",
                "oauth2.client.id", "some_client_id",
                "oauth2.client.secret", "some_client_secret"
        );
        final HttpSinkConfig config = new HttpSinkConfig(configBase);

        final Exception thrown = assertThrows(IllegalArgumentException.class, () ->
            new ApiKeyAccessTokenHttpRequestBuilder().build(config, null).build()
        );
        assertEquals("The expected authorization type is apikey", thrown.getMessage());
    }
}
