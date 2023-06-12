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

package org.geovistory.kafka.connect.http;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.geovistory.kafka.sink.connector.rdf.HttpSinkConnector;
import org.geovistory.toolbox.streams.avro.Operation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class AvroIntegrationTest {

    private static final String HTTP_PATH = "/send-data-here";
    private static final String AUTHORIZATION = "Bearer some-token";
    private static final String CONTENT_TYPE = "application/json";

    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TEST_TOPIC = "dev-rdf-test-topic";
    private static final int TEST_TOPIC_PARTITIONS = 4;

    static final String JSON_PATTERN = "{\"name\":\"%s\",\"value\":\"%s\"}";

    /*static final Schema VALUE_RECORD_SCHEMA = new Schema.Parser()
            .parse("{\"type\":\"record\",\"name\":\"record\","
                    + "\"fields\":["
                    + "{\"name\":\"name\",\"type\":\"string\"}, "
                    + "{\"name\":\"value\",\"type\":\"string\"}]}");*/

    private String schemaRegistryUrl = "https://schema-registry.geovistory.org/";

    private static File pluginsDir;

    private static final String DEFAULT_TAG = "6.0.2";

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("confluentinc/cp-kafka").withTag(DEFAULT_TAG);

    @Container
    private final KafkaContainer kafka = new KafkaContainer(DEFAULT_IMAGE_NAME)
            .withNetwork(Network.newNetwork())
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    private Admin admin;
    private KafkaProducer<String, GenericRecord> producer;

    private ConnectRunner connectRunner;

    private String fusekiUrl;
    private String adminPw = "pw";
    @Container
    private GenericContainer fuseki = new GenericContainer(DockerImageName.parse("ghcr.io/geovistory/fuseki-geosparql:v2.1.0"))
            .withEnv("ADMIN_PASSWORD", adminPw)
            .withExposedPorts(3030);

    @BeforeAll
    static void setUpAll() throws IOException, InterruptedException {
        final File testDir = Files.createTempDirectory("http-connector-for-apache-kafka-").toFile();
        testDir.deleteOnExit();

        pluginsDir = new File(testDir, "plugins/");
        assert pluginsDir.mkdirs();

        // Unpack the library distribution.
        final File transformDir = new File(pluginsDir, "http-connector-for-apache-kafka/");
        assert transformDir.mkdirs();
        final File distFile = new File(System.getProperty("integration-test.distribution.file.path"));
        assert distFile.exists();
        final String cmd = String.format("tar -xf %s --strip-components=1 -C %s", distFile, transformDir);
        final Process p = Runtime.getRuntime().exec(cmd);
        assert p.waitFor() == 0;
    }

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException {
        String address = fuseki.getHost();
        Integer port = fuseki.getFirstMappedPort();
        fusekiUrl = "http://" + address + ":" + port;

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        final Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1",
                "schema.registry.url", schemaRegistryUrl
        );
        producer = new KafkaProducer<>(producerProps);

        try (Admin admin = Admin.create(adminClientConfig)) {
            String topicName = TEST_TOPIC;
            int partitions = TEST_TOPIC_PARTITIONS;
            short replicationFactor = 1;
            // Create a compacted topic
            CreateTopicsResult result = admin.createTopics(Collections.singleton(
                    new NewTopic(topicName, partitions, replicationFactor)
                            .configs(Collections.singletonMap(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT))
            ));
            // Call values() to get the result for a specific topic
            KafkaFuture<Void> future = result.values().get(topicName);
            // Call get() to block until the topic creation is complete or has failed
            // if creation failed the ExecutionException wraps the underlying cause.
            future.get();
        }

        connectRunner = new ConnectRunner(pluginsDir, kafka.getBootstrapServers());
        connectRunner.start();
    }

    @AfterEach
    final void tearDown() {
        connectRunner.stop();
        admin.close();
        producer.close();

        connectRunner.awaitStop();

        fuseki.stop();
    }

    @Test
    @Timeout(30)
    final void testBasicDelivery() throws ExecutionException, InterruptedException {

        connectRunner.createConnector(basicConnectorConfig());

        final List<String> expectedBodies = new ArrayList<>();
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String ttl = "<foo" + i + ">";
                final int projectId = i / 100;
                final var operation = Operation.insert;
                final var record = createRecord(ttl, projectId, operation);
                //expectedBodies.add(String.format(JSON_PATTERN, projectId, operation));
                sendFutures.add(sendMessageAsync(record));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

    }

    private Map<String, String> basicConnectorConfig() {
        final var config = new HashMap<String, String>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", HttpSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", schemaRegistryUrl);
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", schemaRegistryUrl);
        config.put("tasks.max", "1");
        config.put("http.url", "http://" + fuseki.getHost() + ":" + fuseki.getFirstMappedPort() + HTTP_PATH);
        config.put("http.authorization.type", "static");
        config.put("http.headers.authorization", AUTHORIZATION);
        config.put("http.headers.content.type", CONTENT_TYPE);
        return config;
    }

    private ProducerRecord<ProjectRdfKey, ProjectRdfValue> createRecord(final String ttl, final int projectId,
                                                                        final Operation operation) {

        ProjectRdfKey key = ProjectRdfKey.newBuilder().setProjectId(projectId).setTurtle(ttl).build();
        ProjectRdfValue value = ProjectRdfValue.newBuilder().setOperation(operation).build();

        return new ProducerRecord<ProjectRdfKey, ProjectRdfValue>("dev-rdf-test-topic", key, value);
    }

    private Future<RecordMetadata> sendMessageAsync(final ProducerRecord record) {
        return producer.send(record);
    }
}
