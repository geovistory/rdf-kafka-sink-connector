/*
 * Copyright 2019 Aiven Oy and http-connector-for-apache-kafka project contributors
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

package org.geovistory.kafka.sink.connector.rdf.recordsender;

import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.kafka.sink.connector.rdf.config.HttpSinkConfig;
import org.geovistory.kafka.sink.connector.rdf.converter.RecordKeyConverter;
import org.geovistory.kafka.sink.connector.rdf.converter.RecordValueConverter;
import org.geovistory.kafka.sink.connector.rdf.sender.HttpSender;

import java.util.Collection;

public abstract class RecordSender {

    protected final HttpSender httpSender;

    protected final RecordValueConverter recordValueConverter = new RecordValueConverter();
    protected final RecordKeyConverter recordKeyConverter = new RecordKeyConverter();

    protected RecordSender(final HttpSender httpSender) {
        this.httpSender = httpSender;
    }

    public abstract void send(final Collection<SinkRecord> records);

    public abstract void send(final SinkRecord record);

    public static RecordSender createRecordSender(final HttpSender httpSender, final HttpSinkConfig config) {
        if (config.batchingEnabled()) {
            return new BatchRecordSender(
                    httpSender,
                    config.batchMaxSize(),
                    config.batchPrefix(),
                    config.batchSuffix(),
                    config.batchSeparator());
        } else {
            return new SingleRecordSender(httpSender);
        }
    }

}
