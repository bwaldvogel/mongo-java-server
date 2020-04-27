package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TailableCursor extends AbstractCursor {

    private final Aggregation aggregation;
    private long resumeToken;

    public TailableCursor(long cursorId, Aggregation aggregation, long resumeToken) {
        super(cursorId, Collections.emptyList());
        this.aggregation = aggregation;
        this.resumeToken = resumeToken;
        Assert.notNull(aggregation);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        if (numberToReturn <= 0) {
            return takeDocuments();
        }
        List<Document> documents = aggregation.computeResult().stream()
            .filter(d -> getResumeToken(d) > resumeToken)
            .limit(numberToReturn)
            .collect(Collectors.toList());
        updateResumeToken(documents);
        return documents;
    }

    private List<Document> takeDocuments() {
        List<Document> documents = aggregation.computeResult().stream()
            .filter(d -> getResumeToken(d) > resumeToken)
            .collect(Collectors.toList());
        updateResumeToken(documents);
        return documents;
    }

    private void updateResumeToken(List<Document> documents) {
        if (documents.size() > 0) {
            resumeToken = getResumeToken(documents.get(documents.size() - 1));
        }
    }

    private long getResumeToken(Document doc) {
        return Long.parseLong((String) ((Document)doc.get("_id")).get("_data"), 16);
    }
}
