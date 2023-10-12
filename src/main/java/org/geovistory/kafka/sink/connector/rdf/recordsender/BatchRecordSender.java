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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geovistory.toolbox.streams.avro.Operation;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

final class BatchRecordSender extends RecordSender {
    private final int batchMaxSize;
    private final String batchPrefix;
    private final String batchSuffix;
    private final String batchSeparator;
    private static final Logger log = LoggerFactory.getLogger(BatchRecordSender.class);

    BatchRecordSender(final HttpSender httpSender,
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
        log.info("Preparing batches from " + records.size() + " consumed records...");
        List<List<RdfRecord>> batches = new ArrayList<>();

        Map<Integer, List<RdfRecord>> insertBatchesByProject = null; // buffer of insert batches grouped by projectId
        Map<Integer, List<RdfRecord>> deleteBatchesByProject = null; // buffer of delete batches grouped by projectId

        List<RdfRecord> currentBatch = new ArrayList<>(batchMaxSize);
        Operation currentOperation = null;
        Integer currentProjectId = null;

        // we iterate over records and group them by project id
        // thereby we ensure there are never insert and delete records
        // in the two buffers at the same time. This way we keep the
        // correct order of operations per dataset.

        for (final var record : records) {
            var key = recordKeyConverter.convert(record);
            Integer projectId = key.getProjectId();

            var value = recordValueConverter.convert(record);
            var operation = value.getOperation();

            var r = new RdfRecord(key, value);

            if (operation == Operation.insert) {
                // if there is a delete batch for the project id
                if (deleteBatchesByProject != null && deleteBatchesByProject.containsKey(projectId)) {
                    // add the delete batch to batches
                    batches.add(deleteBatchesByProject.get(projectId));
                    // and clean up
                    deleteBatchesByProject.remove(projectId);
                }

                // if there is no insert batch for the project id
                if (!insertBatchesByProject.containsKey(projectId)) {
                    // initialize a new list
                    insertBatchesByProject.put(projectId, new ArrayList<>());
                }
                insertBatchesByProject.get(projectId).add(r);
            } else if (operation == Operation.delete) {
                // if there is a insert batch for the project id
                if (insertBatchesByProject != null && insertBatchesByProject.containsKey(projectId)) {
                    // add the insert batch to batches
                    batches.add(insertBatchesByProject.get(projectId));
                    // and clean up
                    insertBatchesByProject.remove(projectId);
                }
                // if there is no delete batch for the project id
                if (!deleteBatchesByProject.containsKey(projectId)) {
                    // initialize a new list
                    deleteBatchesByProject.put(projectId, new ArrayList<>());
                }
                deleteBatchesByProject.get(projectId).add(r);
            }
        }

        if (insertBatchesByProject != null) {
            for (Map.Entry<Integer, List<RdfRecord>> item : insertBatchesByProject.entrySet()) {
                batches.add(item.getValue());
            }
        }

        if (deleteBatchesByProject != null) {
            for (Map.Entry<Integer, List<RdfRecord>> item : deleteBatchesByProject.entrySet()) {
                batches.add(item.getValue());
            }
        }


        for (List<RdfRecord> nextBatch : batches) {
            var firstRecord = nextBatch.get(0);
            var projectId = Integer.toString(firstRecord.key.getProjectId());
            var operation = firstRecord.value.getOperation();
            final String body = createRequestBody(nextBatch, operation, batchPrefix, batchSeparator, batchSuffix);
            log.info("Sending " + nextBatch.size() + " records for project " + projectId + "...");
            httpSender.send(body, projectId);
        }

    }

    @Override
    public void send(final SinkRecord record) {
        throw new ConnectException("Don't call this method for batch sending");
    }

    static String createRequestBody(
            final Collection<RdfRecord> batch,
            Operation operation,
            String batchPrefix,
            String batchSeparator,
            String batchSuffix) {
        final StringBuilder result = new StringBuilder();
        if (!batchPrefix.isEmpty()) {
            result.append(batchPrefix);
        }

        if (operation.equals(Operation.insert)) {
            result.append("update=INSERT DATA { ");
        } else if (operation.equals(Operation.delete)) {
            result.append("update=DELETE DATA { ");
        }

        final Iterator<RdfRecord> it = batch.iterator();
        if (it.hasNext()) {
            var key = it.next().key;
            var turtle = key.getTurtle().stripTrailing();
            result.append(URLEncoder.encode(turtle, StandardCharsets.UTF_8));
            while (it.hasNext()) {
                key = it.next().key;
                turtle = URLEncoder.encode(key.getTurtle(), StandardCharsets.UTF_8).stripTrailing();
                if (!result.substring(result.length() - 1).equals(batchSeparator)) {
                    result.append(batchSeparator);
                }
                result.append(turtle);
            }
        }
        if (!batchSuffix.isEmpty()) {
            result.append(batchSuffix);
        }
        result.append(" }");
        return result.toString();
    }
}
