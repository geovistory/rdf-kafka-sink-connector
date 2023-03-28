package org.geovistory.kafka.sink.connector.rdf.config;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.assertj.core.api.Assertions.from;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;

public class HttpSinkConfigTest {
    @Test
    void testRequiredConfigurations(){
        final Map<String, String> properties = Map.of();
        assertThatExceptionOfType(ConfigException.class)
                .describedAs("Expected config exception due to missing http.url")
                .isThrownBy(() -> new HttpSinkConfig(properties))
                .withMessage("Missing required configuration \"http.url\" which has no default value.");
    }

    @Test
    void correctMinimalConfig() throws URISyntaxException {
        final Map<String, String> properties = Map.of(
                "http.url", "http://localhost:8090",
                "http.authorization.type", "none"
        );

        final HttpSinkConfig config = new HttpSinkConfig(properties);
        assertThat(config)
                .returns(new URI("http://localhost:8090"), from(HttpSinkConfig::httpUri))
                .returns(AuthorizationType.NONE, from(HttpSinkConfig::authorizationType))
                .returns(null, from(HttpSinkConfig::headerContentType))
                .returns(false, from(HttpSinkConfig::batchingEnabled))
                .returns(500, from(HttpSinkConfig::batchMaxSize))
                .returns(1, from(HttpSinkConfig::maxRetries))
                .returns(3000, from(HttpSinkConfig::retryBackoffMs))
                .returns(Collections.emptyMap(), from(HttpSinkConfig::getAdditionalHeaders))
                .returns(null, from(HttpSinkConfig::oauth2ClientId))
                .returns(null, from(HttpSinkConfig::oauth2ClientSecret))
                .returns(null, from(HttpSinkConfig::oauth2ClientScope))
                .returns(OAuth2AuthorizationMode.HEADER, from(HttpSinkConfig::oauth2AuthorizationMode))
                .returns("access_token", from(HttpSinkConfig::oauth2ResponseTokenProperty))
                .returns(null, from(HttpSinkConfig::kafkaRetryBackoffMs));
    }

}
