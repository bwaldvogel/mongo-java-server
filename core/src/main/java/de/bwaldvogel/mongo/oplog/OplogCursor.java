package de.bwaldvogel.mongo.oplog;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import de.bwaldvogel.mongo.backend.CollectionUtils;
import de.bwaldvogel.mongo.backend.Cursor;
import de.bwaldvogel.mongo.bson.Document;

public class OplogCursor extends Cursor {

    private final Function<OplogPosition, Stream<Document>> oplogStream;
    private OplogPosition position;
    private boolean hasSeenTerminalEvent;

    public OplogCursor(long cursorId, Function<OplogPosition, Stream<Document>> oplogStream, OplogPosition position) {
        this(cursorId, oplogStream, position, false);
    }

    private OplogCursor(long cursorId, Function<OplogPosition, Stream<Document>> oplogStream, OplogPosition position,
                       boolean hasSeenTerminalEvent) {
        super(cursorId);
        this.oplogStream = oplogStream;
        this.position = position;
        this.hasSeenTerminalEvent = hasSeenTerminalEvent;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        if (hasSeenTerminalEvent) {
            return invalidateStream();
        }
        Stream<Document> stream = oplogStream.apply(position);

        if (numberToReturn > 0) {
            stream = stream.limit(numberToReturn);
        }

        List<Document> documents = stream.collect(Collectors.toList());
        updatePosition(documents);
        return documents;
    }

    public List<Document> invalidateStream() {
        Document result = new Document()
                .append(OplogDocumentFields.ID, new Document(OplogDocumentFields.ID_DATA_KEY, position.toHexString()))
                .append("operationType", OperationType.INVALIDATE.getDescription());
        return Collections.singletonList(result);
    }

    @Override
    public OplogCursor invalidate() {
        return new OplogCursor(0, oplogStream, position, true);
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
