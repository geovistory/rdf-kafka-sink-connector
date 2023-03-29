package org.geovistory.kafka.sink.connector.rdf.converter;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class RecordValueConverterTest {
    final RecordValueConverter recordValueConverter = new RecordValueConverter();

    @Test
    void convertAvroRecord() {
        final var recordSchema = SchemaBuilder.struct()
                .name("record")
                .field("name", SchemaBuilder.string())
                .field("value", SchemaBuilder.string());

        final var value = new Struct(recordSchema);
        value.put(new Field("name", 0, SchemaBuilder.string()), "user-0");
        value.put(new Field("value", 1, SchemaBuilder.string()), "value-0");

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord))
                .isEqualTo("{\"name\":\"user-0\",\"value\":\"value-0\"}");
    }

    @Test
    void convertStringRecord() {
        final var recordSchema = SchemaBuilder.string();

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, "some-str-value", 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("some-str-value");
    }

    @Test
    void convertHashMapRecord() {
        final var recordSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

        final Map<String, String> value = new HashMap<>();
        value.put("key", "value");

        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(),
                "some-key", recordSchema, value, 1L);

        assertThat(recordValueConverter.convert(sinkRecord)).isEqualTo("{\"key\":\"value\"}");
    }


    @Test
    void throwsDataExceptionForUnknownRecordValueClass() {
        final var recordSchema = SchemaBuilder.int64();
        final var sinkRecord = new SinkRecord(
                "some-topic", 0,
                SchemaBuilder.string(), "some-key",
                recordSchema, 42L, 1L);

        assertThatExceptionOfType(DataException.class)
                .isThrownBy(() -> recordValueConverter.convert(sinkRecord)).isInstanceOf(DataException.class);
    }
}
