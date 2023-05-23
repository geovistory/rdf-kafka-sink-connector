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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;
import java.util.Map;

public class RecordKeyConverter {

    private final JsonRecordKeyConverter jsonRecordKeyConverter = new JsonRecordKeyConverter();

    private final Map<Class<?>, Converter> converters = Map.of(
            String.class, record -> (String) record.key(),
            HashMap.class, jsonRecordKeyConverter,
            Struct.class, jsonRecordKeyConverter
    );

    interface Converter {
        String convert(final SinkRecord record);
    }

    public String convert(final SinkRecord record) {
        if (!converters.containsKey(record.key().getClass())) {
            throw new DataException(
                    "Record key must be String, Schema Struct or HashMap,"
                    + " but " + record.key().getClass() + " is given");
        }
        return converters.get(record.key().getClass()).convert(record);
    }

}
