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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.geovistory.kafka.sink.connector.rdf.sender.HttpSender;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

final class BatchRecordSender extends RecordSender {
    private final int batchMaxSize;
    private final String batchPrefix;
    private final String batchSuffix;
    private final String batchSeparator;

    protected BatchRecordSender(final HttpSender httpSender,
                                final int batchMaxSize,
                                final String batchPrefix,
                                final String batchSuffix,
                                final String batchSeparator) {
        super(httpSender);
        this.batchMaxSize = batchMaxSize;
        this.batchPrefix = batchPrefix;
        this.batchSuffix = batchSuffix;
        this.batchSeparator = batchSeparator;
    }

    @Override
    public void send(final Collection<SinkRecord> records) {
        //final List<SinkRecord> batch = new ArrayList<>(batchMaxSize);
        List<List<SinkRecord>> batches = new ArrayList<>();
        List<SinkRecord> currentBatch = new ArrayList<>(batchMaxSize);
        String currentOperation = null;
        String currentProjectId = null;

        for (final var record : records) {
            var jsonKey = new JSONObject(recordKeyConverter.convert(record));
            var projectId = jsonKey.get("project_id").toString();
            var turtle = jsonKey.get("turtle").toString();

            var jsonValue = new JSONObject(recordValueConverter.convert(record));
            var operation = jsonValue.get("operation").toString();

            if (!operation.equals(currentOperation) || !projectId.equals(currentProjectId) || currentBatch.size() >= batchMaxSize) {
                // Create a new batch if:
                // - Operation is different from the current batch, or
                // - Project ID is different from the current batch, or
                // - Current batch size exceeds the maximum batch size

                if (!currentBatch.isEmpty()) {
                    // Add the current batch to the list of batches
                    batches.add(new ArrayList<>(currentBatch));
                    currentBatch.clear();
                }

                // Start a new batch with the current message
                currentOperation = operation;
                currentProjectId = projectId;
                currentBatch.add(record);
            } else {
                // Add the message to the current batch
                currentBatch.add(record);
            }
        }

        // Add the last batch to the list of batches
        if (!currentBatch.isEmpty()) {
            batches.add(new ArrayList<>(currentBatch));
        }

        for (List<SinkRecord> batch : batches) {
            final String body = createRequestBody(batch);
            var firstRecord = batch.get(0);
            var jsonKey = new JSONObject(recordKeyConverter.convert(firstRecord));
            var projectId = jsonKey.get("project_id").toString();
            httpSender.send(body, projectId);
        }
    }

    @Override
    public void send(final SinkRecord record) {
        throw new ConnectException("Don't call this method for batch sending");
    }

    private String createRequestBody(final Collection<SinkRecord> batch) {
        //TODO change the way the body is build to have the correct SPARQL query
        final StringBuilder result = new StringBuilder();
        if (!batchPrefix.isEmpty()) {
            result.append(batchPrefix);
        }
        final Iterator<SinkRecord> it = batch.iterator();
        if (it.hasNext()) {
            result.append(recordValueConverter.convert(it.next()));
            while (it.hasNext()) {
                result.append(batchSeparator);
                result.append(recordValueConverter.convert(it.next()));
            }
        }
        if (!batchSuffix.isEmpty()) {
            result.append(batchSuffix);
        }
        return result.toString();
    }
}
