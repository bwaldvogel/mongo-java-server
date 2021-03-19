package de.bwaldvogel.mongo.oplog;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.backend.AbstractCursor;
import de.bwaldvogel.mongo.bson.Document;

class InvalidateOplogCursor extends AbstractCursor {
    private final OplogPosition position;

    InvalidateOplogCursor(OplogPosition position) {
        super(0);
        this.position = position;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        Document result = new Document()
            .append(OplogDocumentFields.ID, new Document(OplogDocumentFields.ID_DATA_KEY, position.toHexString()))
            .append("operationType", OperationType.INVALIDATE.getDescription());
        return Collections.singletonList(result);
    }

}
