package org.geovistory.kafka.sink.connector.rdf.fuseki;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.http.HttpClient;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class DatasetHandlerTest {
    String fusekiUrl;
    String adminPw = "pw";
    @Container
    public GenericContainer fuseki = new GenericContainer(DockerImageName.parse("ghcr.io/geovistory/fuseki-geosparql:v2.1.0"))
            //public GenericContainer fuseki = new GenericContainer(DockerImageName.parse("secoresearch/fuseki"))
            .withEnv("ADMIN_PASSWORD", adminPw)
            .withExposedPorts(3030);

    @BeforeEach
    public void setUp() {
        String address = fuseki.getHost();
        Integer port = fuseki.getFirstMappedPort();
        fusekiUrl = "http://" + address + ":" + port;
    }

    @Test
    public void testFusekiIsUp() throws IOException {

        URL url = new URL(fusekiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        Integer responseCode = connection.getResponseCode();
        assertEquals(responseCode, 200);
    }

    @Test
    public void testCreateFusekiDataset() throws IOException {
        // Create the dataset
        Integer responseCode = DatasetHandler.createFusekiDataset("my-dataset", fusekiUrl, "admin:" + adminPw);

        // Check if it exists

        assertEquals(responseCode, 200);
    }


}