package org.geovistory.kafka.sink.connector.rdf.converter;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordKeyConverterTest {
    final RecordKeyConverter recordKeyConverter = new RecordKeyConverter();

    @Test
    void convertAvroRecord() {

        var avroKey = ProjectRdfKey.newBuilder().setProjectId(12).setTurtle("<a> <b> <c>").build();
        var avroData = new AvroData(2);
        var sv = avroData.toConnectData(avroKey.getSchema(), avroKey);
        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                sv.schema(), sv.value(),
                SchemaBuilder.string(),
                "some-value", 1L);

        assertThat(recordKeyConverter.convert(sinkRecord))
                .isEqualTo(avroKey);
    }


}
