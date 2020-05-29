package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.oplog.ResumeToken;

public class TailableCursor extends AbstractCursor {

    private final Function<ResumeToken, Stream<Document>> oplogStream;
    private ResumeToken resumeToken;

    public TailableCursor(long cursorId, Function<ResumeToken, Stream<Document>> oplogStream, ResumeToken resumeToken) {
        super(cursorId, Collections.emptyList());
        this.oplogStream = oplogStream;
        this.resumeToken = resumeToken;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        Stream<Document> stream = oplogStream.apply(resumeToken);

        if (numberToReturn > 0) {
            stream = stream.limit(numberToReturn);
        }

        List<Document> documents = stream.collect(Collectors.toList());
        updateResumeToken(documents);
        return documents;
    }

    private void updateResumeToken(List<Document> documents) {
        if (!documents.isEmpty()) {
            resumeToken = getResumeToken(CollectionUtils.getLastElement(documents));
        }
    }

    private static ResumeToken getResumeToken(Document document) {
        Document id = (Document) document.get("_id");
        return ResumeToken.fromDocument(id);
    }

}
