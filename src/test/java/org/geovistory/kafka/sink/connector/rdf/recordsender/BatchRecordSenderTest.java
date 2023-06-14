package org.geovistory.kafka.sink.connector.rdf.recordsender;

import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BatchRecordSenderTest {


    @Test
    void createBatches() {


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
                "<foo> <p> <bar>." +
                "<foo2> <p> <bar2>" +
                " }";

        var result = BatchRecordSender.createRequestBody(batch, Operation.insert, "", ".", "");

        assertEquals(expected, result);
    }


}
