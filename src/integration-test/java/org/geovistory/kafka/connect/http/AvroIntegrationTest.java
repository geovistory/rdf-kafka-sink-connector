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

import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.geovistory.kafka.sink.connector.rdf.HttpSinkConnector;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.jetbrains.annotations.NotNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Run the connector in kafka connect and test the integration with
 * Kafka Broker, Schema Registry and Fuseki
 */
@Testcontainers
public class AvroIntegrationTest {
    private static final String HTTP_AUTHORIZATION_TYPE_CONFIG = "static";
    private static final String AUTHORIZATION = "admin:pw";
    private static final String CONTENT_TYPE = "application/x-www-form-urlencoded";

    private static final String CONNECTOR_NAME = "test-source-connector";

    private static final String TEST_TOPIC = "project-rdf";
    private static final int TEST_TOPIC_PARTITIONS = 4;

    private static File pluginsDir;

    private static final DockerImageName DEFAULT_IMAGE_NAME =
            DockerImageName.parse("docker.redpanda.com/redpandadata/redpanda:v23.1.2");

    @Container
    private final RedpandaContainer redpandaContainer = new RedpandaContainer(DEFAULT_IMAGE_NAME)
            .withNetwork(Network.newNetwork())
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    public Admin admin;
    private KafkaProducer<String, GenericRecord> producer;

    private ConnectRunner connectRunner;

    private String fusekiUrl;
    private final String adminPw = "pw";

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
    void setUp() {
        String address = fuseki.getHost();
        Integer port = fuseki.getFirstMappedPort();
        fusekiUrl = "http://" + address + ":" + port;

        final Properties adminClientConfig = new Properties();
        adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaContainer.getBootstrapServers());

        final Map<String, Object> producerProps = Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, redpandaContainer.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer",
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1",
                "schema.registry.url", redpandaContainer.getSchemaRegistryAddress()
        );
        producer = new KafkaProducer<>(producerProps);

        try {
            admin = Admin.create(adminClientConfig);
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        connectRunner = new ConnectRunner(pluginsDir, redpandaContainer.getBootstrapServers());
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

    /**
     * Test the single record sender
     *
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    @Timeout(30)
    final void testSingleMode() throws ExecutionException, InterruptedException, IOException {
        var config = basicConnectorConfig();
        config.put("batching.enabled", "false");
        connectRunner.createConnector(config);

        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        // insert 10 triples
        for (int i = 0; i < 10; i++) {
            for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
                final String ttl = "<foo" + i + "> <has> <bar>";

                // insert the first 6 to project 11, the rest in 99
                final int projectId = i < 6 ? 11 : 99;
                final var operation = Operation.insert;
                final var record = createRecord(ttl, projectId, operation);
                sendFutures.add(sendMessageAsync(record));
            }
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }


        TimeUnit.SECONDS.sleep(10);

        // get a list of fuseki datasets
        JSONArray datasets = getListOfDatasets();

        // get all triples in project 99
        JSONArray triples99 = sparql("api_v1_project_99", "SELECT * WHERE { ?sub ?pred ?obj . }");

        // get all triples in project 11
        JSONArray triples11 = sparql("api_v1_project_11", "SELECT * WHERE { ?sub ?pred ?obj . }");


        // assert it returns the 2 created datasets plus the default dataset
        assertThat(datasets.length()).isEqualTo(3);

        // assert project 11 has 6 triples
        assertThat(triples11.length()).isEqualTo(6);

        // assert project 99 has 4 triples
        assertThat(triples99.length()).isEqualTo(4);


    }

    /**
     * Test the batch record sender
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    @Timeout(100)
    final void testBatchMode() throws ExecutionException, InterruptedException, IOException {
        var config = basicConnectorConfig();
        config.put("batching.enabled", "true");
        connectRunner.createConnector(config);
        final List<Future<RecordMetadata>> sendFutures = new ArrayList<>();

        for (int i = 0; i < 1000; i++) {
            // for (int partition = 0; partition < TEST_TOPIC_PARTITIONS; partition++) {
            final String ttl = "<foo" + i + "> <has> <bar>";
            final int projectId = i / 100;
            final var operation = Operation.insert;
            final var record = createRecord(ttl, projectId, operation);
            //expectedBodies.add(String.format(JSON_PATTERN, projectId, operation));
            sendFutures.add(sendMessageAsync(record));
            //}
        }
        producer.flush();
        for (final Future<RecordMetadata> sendFuture : sendFutures) {
            sendFuture.get();
        }

        TimeUnit.SECONDS.sleep(10);

        // get a list of fuseki datasets
        JSONArray datasets = getListOfDatasets();

        // get all triples in project 1
        JSONArray triples1 = sparql("api_v1_project_1", "SELECT * WHERE { ?sub ?pred ?obj . }");

        // assert it returns the 10 created datasets plus the default dataset
        assertThat(datasets.length()).isEqualTo(11);

        // assert project 1 has 100 triples
        assertThat(triples1.length()).isEqualTo(100);

    }

    @NotNull
    private JSONArray getListOfDatasets() throws IOException {
        HttpUriRequest request = new HttpGet(fusekiUrl + "/$/datasets");
        String base64 = Base64.getEncoder().encodeToString(("admin:" + adminPw).getBytes(StandardCharsets.UTF_8));
        request.addHeader("Authorization", "Basic " + base64);
        // When

        HttpResponse response = HttpClientBuilder.create().build().execute(request);
        String json = EntityUtils.toString(response.getEntity());
        var jsonObject = new JSONObject(json);
        var datasets = new JSONArray(jsonObject.get("datasets").toString());
        return datasets;
    }

    @NotNull
    private JSONArray sparql(String datasetName, String sparqlSelect) throws IOException {
        HttpUriRequest request = new HttpPost(fusekiUrl + "/" + datasetName + "?query=" + URLEncoder.encode(sparqlSelect));
        request.addHeader("Content-Type", "application/x-www-form-urlencoded");
        request.addHeader("Accept", "application/sparql-results+json");

        String base64 = Base64.getEncoder().encodeToString(("admin:" + adminPw).getBytes(StandardCharsets.UTF_8));
        request.addHeader("Authorization", "Basic " + base64);

        HttpResponse response = HttpClientBuilder.create().build().execute(request);
        String json = EntityUtils.toString(response.getEntity());
        var jsonObject = new JSONObject(json);
        var bindings = jsonObject.getJSONObject("results").getJSONArray("bindings");
        return bindings;
    }

    private Map<String, String> basicConnectorConfig() {
        final var config = new HashMap<String, String>();
        config.put("name", CONNECTOR_NAME);
        config.put("connector.class", HttpSinkConnector.class.getName());
        config.put("topics", TEST_TOPIC);
        config.put("key.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("key.converter.schema.registry.url", redpandaContainer.getSchemaRegistryAddress());
        config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
        config.put("value.converter.schema.registry.url", redpandaContainer.getSchemaRegistryAddress());
        config.put("tasks.max", "1");
        config.put("http.url", "http://" + fuseki.getHost() + ":" + fuseki.getFirstMappedPort());
        config.put("http.endpoint", "api_v1_community_data");
        config.put("http.projects.endpoint", "api_v1_project_");
        config.put("http.authorization.type", HTTP_AUTHORIZATION_TYPE_CONFIG);
        config.put("http.headers.authorization", AUTHORIZATION);
        config.put("http.headers.content.type", CONTENT_TYPE);
        config.put("retry.backoff.ms", "100");
        config.put("batching.enabled", "true");
        config.put("batch.separator", ".");

        return config;
    }

    private ProducerRecord<ProjectRdfKey, ProjectRdfValue> createRecord(final String ttl, final int projectId,
                                                                        final Operation operation) {

        ProjectRdfKey key = ProjectRdfKey.newBuilder().setProjectId(projectId).setTurtle(ttl).build();
        ProjectRdfValue value = ProjectRdfValue.newBuilder().setOperation(operation).build();

        return new ProducerRecord<>("project-rdf", key, value);
    }

    private Future<RecordMetadata> sendMessageAsync(final ProducerRecord record) {
        return producer.send(record);
    }
}
