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

package org.geovistory.kafka.sink.connector.rdf.converter;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AvroRecordValueConverter implements RecordValueConverter.Converter {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordValueConverter.class);
    private final AvroData avroData = new AvroData(new AvroDataConfig.Builder()
            .with(AvroDataConfig.SCHEMAS_CACHE_SIZE_CONFIG, 1000)
            .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
            .build());

    private final Schema projectRdfValueSchema = avroData.toConnectSchema(ProjectRdfValue.SCHEMA$);

    @Override
    public ProjectRdfValue convert(final SinkRecord record) {
        try {
            var gerneric = (GenericData.Record) avroData.fromConnectData(projectRdfValueSchema, record.value());
            Operation o = (Operation) gerneric.get("operation");
            if (o == null) {
                throw new Exception("record.value.operation is null");
            }
            return ProjectRdfValue.newBuilder().setOperation(o).build();
        } catch (Exception e) {
            log.error("Unable to convert record.value in topic {} at offset {} with value {}", record.topic(), record.kafkaOffset(), record.value());
            log.error(e.getMessage());
        }
        return null;
    }

}
