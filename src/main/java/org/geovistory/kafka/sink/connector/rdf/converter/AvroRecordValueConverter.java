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

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.toolbox.streams.avro.Operation;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AvroRecordValueConverter implements RecordValueConverter.Converter {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordValueConverter.class);

    @Override
    public ProjectRdfValue convert(final SinkRecord record) {
        try {
            var generic = (Struct) record.value();
            var operationStr = generic.get("operation").toString();
            Operation o = null;
            if (operationStr.equals("insert")) {
                o = Operation.insert;
            } else if (operationStr.equals("delete")) {
                o = Operation.delete;
            }
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
