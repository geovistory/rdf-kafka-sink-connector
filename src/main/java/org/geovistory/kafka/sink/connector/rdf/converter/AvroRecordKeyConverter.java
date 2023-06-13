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
import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AvroRecordKeyConverter implements RecordKeyConverter.Converter {
    private static final Logger log = LoggerFactory.getLogger(AvroRecordKeyConverter.class);

    @Override
    public ProjectRdfKey convert(final SinkRecord record) {
        try {
            var generic = (Struct) record.key();
            Integer p = (Integer) generic.get("project_id");
            String t = (String) generic.get("turtle");

            // id=0 if null
            if (p == null) p = 0;

            // validate turtle
            if (t == null) {
                throw new Exception("record.key.turtle is null");
            }

            return ProjectRdfKey.newBuilder().setProjectId(p).setTurtle(t).build();

        } catch (Exception e) {
            log.error("Unable to convert record.key in topic {} at offset {} with key {}", record.topic(), record.kafkaOffset(), record.key());
            log.error(e.getMessage());
        }
        return null;
    }
}
