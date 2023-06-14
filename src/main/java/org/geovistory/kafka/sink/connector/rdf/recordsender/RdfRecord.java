package org.geovistory.kafka.sink.connector.rdf.recordsender;

import org.geovistory.toolbox.streams.avro.ProjectRdfKey;
import org.geovistory.toolbox.streams.avro.ProjectRdfValue;

public class RdfRecord {
    public ProjectRdfKey key;
    public ProjectRdfValue value;

    public RdfRecord(ProjectRdfKey key, ProjectRdfValue value) {
        this.key = key;
        this.value = value;
    }
}
