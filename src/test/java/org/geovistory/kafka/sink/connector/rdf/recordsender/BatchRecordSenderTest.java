package org.geovistory.kafka.sink.connector.rdf.recordsender;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.kafka.sink.connector.rdf.config.HttpSinkConfig;
import org.geovistory.kafka.sink.connector.rdf.sender.HttpSender;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchRecordSenderTest {


    @Test
    void createRequestBody() {


        List<RdfRecord> batch = List.of(
                new RdfRecord(
                        ProjectRdfKey.newBuilder().setProjectId(1).setTurtle("<foo> <p> <bar>").build(),
                        ProjectRdfValue.newBuilder().setOperation(Operation.insert).build()
                ),
                new RdfRecord(
                        ProjectRdfKey.newBuilder().setProjectId(1).setTurtle("<foo2> <p> <bar2>").build(),
                        ProjectRdfValue.newBuilder().setOperation(Operation.insert).build()
                ));

        String expected = "update=INSERT DATA { " +
                URLEncoder.encode("<foo> <p> <bar>.", StandardCharsets.UTF_8) +
                URLEncoder.encode("<foo2> <p> <bar2>", StandardCharsets.UTF_8) +
                " }";

        var batches = BatchRecordSender.createRequestBody(batch, Operation.insert, "", ".", "");

        assertEquals(expected, batches);
    }

    /**
     * It should create one batch for project 1 and one for project 12
     */
    @Test
    void shouldCreateTwoBatches() {

        var input = List.of(
                toSinkRecord(12, "<a> <b> <c>", Operation.insert),
                toSinkRecord(1, "<a> <b> <c>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c>", Operation.insert)
        );

        var batches = getBatchRecordSender(500).createBatches(input);

        assertEquals(2, batches.size());
        assertEquals(1, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
        assertEquals(12, batches.get(1).get(0).key.getProjectId());
    }

    /**
     * It should create one batch for project 12 insert, one for project 1 delete, and one for project 12 delete
     */
    @Test
    void shouldCreateThreeBatches() {

        var input = List.of(
                toSinkRecord(12, "<a> <b> <c>", Operation.insert),
                toSinkRecord(1, "<a> <b> <c>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c>", Operation.delete),
                toSinkRecord(12, "<a> <b> <c2>", Operation.delete)
        );

        var batches = getBatchRecordSender(500).createBatches(input);

        assertEquals(3, batches.size());
        assertEquals(1, batches.get(0).size());
        assertEquals(Operation.insert, batches.get(0).get(0).value.getOperation());
        assertEquals(1, batches.get(1).size());
        assertEquals(2, batches.get(2).size());
        assertEquals(Operation.delete, batches.get(2).get(0).value.getOperation());
    }

    /**
     * It should create 3 batches for 5 input records with max batch size 2
     */
    @Test
    void shouldRespectMaxBatchSizeForInsert() {

        var input = List.of(
                toSinkRecord(12, "<a> <b> <c1>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c2>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c3>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c4>", Operation.insert),
                toSinkRecord(12, "<a> <b> <c5>", Operation.insert)
        );

        var batches = getBatchRecordSender(2).createBatches(input);

        assertEquals(3, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
        assertEquals(1, batches.get(2).size());
    }

    /**
     * It should create 3 batches for 5 input records with max batch size 2
     */
    @Test
    void shouldRespectMaxBatchSizeForDelete() {

        var input = List.of(
                toSinkRecord(12, "<a> <b> <c1>", Operation.delete),
                toSinkRecord(12, "<a> <b> <c2>", Operation.delete),
                toSinkRecord(12, "<a> <b> <c3>", Operation.delete),
                toSinkRecord(12, "<a> <b> <c4>", Operation.delete),
                toSinkRecord(12, "<a> <b> <c5>", Operation.delete)
        );

        var batches = getBatchRecordSender(2).createBatches(input);

        assertEquals(3, batches.size());
        assertEquals(2, batches.get(0).size());
        assertEquals(2, batches.get(1).size());
        assertEquals(1, batches.get(2).size());
    }


    @NotNull
    private static BatchRecordSender getBatchRecordSender(int maxBatchSize) {
        return new BatchRecordSender(getHttpSender(), maxBatchSize, "", "", ".");
    }

    @NotNull
    private static SinkRecord toSinkRecord(int projectId, String turtle, Operation operation) {
        var avroKey = ProjectRdfKey.newBuilder().setProjectId(projectId).setTurtle(turtle).build();
        var avroValue = ProjectRdfValue.newBuilder().setOperation(operation).build();
        var avroData = new AvroData(2);
        var k = avroData.toConnectData(avroKey.getSchema(), avroKey);
        var v = avroData.toConnectData(avroValue.getSchema(), avroValue);
        return new SinkRecord(
                "some-topic", 0,
                k.schema(), k.value(),
                v.schema(), v.value(), 1L);
    }

    @NotNull
    private static HttpSender getHttpSender() {
        final var config = new HashMap<String, String>();
        config.put("http.url", "https://foo.bar");
        config.put("http.authorization.type", "none");
        return HttpSender.createHttpSender(new HttpSinkConfig(new HashMap<>(config)));
    }

}
