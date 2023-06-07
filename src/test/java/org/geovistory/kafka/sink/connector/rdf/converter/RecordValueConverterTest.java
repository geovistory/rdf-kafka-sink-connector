package org.geovistory.kafka.sink.connector.rdf.converter;

import io.confluent.connect.avro.AvroData;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordValueConverterTest {
    final RecordValueConverter recordValueConverter = new RecordValueConverter();

    @Test
    void convertAvroRecord() {

        var avroVal = ProjectRdfValue.newBuilder().setOperation(Operation.insert).build();
        var avroData = new AvroData(2);
        var sv = avroData.toConnectData(avroVal.getSchema(), avroVal);
        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", sv.schema(), sv.value(), 1L);

        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo(avroVal);
    }


}
