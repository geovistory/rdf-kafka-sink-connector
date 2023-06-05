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
import org.geovistory.kafka.sink.connector.rdf.sender.HttpSender;
import org.geovistory.toolbox.streams.avro.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

final class SingleRecordSender extends RecordSender {

    SingleRecordSender(final HttpSender httpSender) {
        super(httpSender);
    }

    private static final Logger log = LoggerFactory.getLogger(SingleRecordSender.class);

    @Override
    public void send(final Collection<SinkRecord> records) {
        for (final SinkRecord record : records) {
            prepareAndSendBody(record);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        log.info(recordKeyConverter.convert(record).toString());
        prepareAndSendBody(record);
    }

    private void prepareAndSendBody(SinkRecord record) {
        var paramName = "update";
        var sparqlQuery = "";
        var key = recordKeyConverter.convert(record);

        // TODO handle key==null (log a warning and return)

        var projectId = Integer.toString(key.getProjectId());
        var turtle = key.getTurtle();

        var value = recordValueConverter.convert(record);
        // TODO handle value==null (log a warning and return)

        var operation = value.getOperation();

        log.info("operation: " + operation);
        log.info("projectId: " + projectId);
        log.info("turtle: " + turtle);

        if (operation.equals(Operation.insert)) {
            sparqlQuery = "INSERT DATA { " + turtle + "}";
        } else if (operation.equals(Operation.delete)) {
            sparqlQuery = "DELETE DATA { " + turtle + "}";
        }
        var body = paramName + "=" + sparqlQuery;
        log.info(paramName + "=" + sparqlQuery);

        httpSender.send(body, projectId);
    }
}
