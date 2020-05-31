package de.bwaldvogel.mongo.oplog;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.bson.Document;

class OplogCursor extends Cursor {

    private final Function<OplogPosition, Stream<Document>> oplogStream;
    private OplogPosition position;

    public OplogCursor(long cursorId, Function<OplogPosition, Stream<Document>> oplogStream, OplogPosition position) {
        super(cursorId);
        this.oplogStream = oplogStream;
        this.position = position;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        Stream<Document> stream = oplogStream.apply(position);

        if (numberToReturn > 0) {
            stream = stream.limit(numberToReturn);
        }

        List<Document> documents = stream.collect(Collectors.toList());
        updatePosition(documents);
        return documents;
    }

    private void updatePosition(List<Document> documents) {
        if (!documents.isEmpty()) {
            position = getOplogPosition(CollectionUtils.getLastElement(documents));
        }
    }

    private static OplogPosition getOplogPosition(Document document) {
        Document id = (Document) document.get(OplogDocumentFields.ID);
        return OplogPosition.fromDocument(id);
    }

}
