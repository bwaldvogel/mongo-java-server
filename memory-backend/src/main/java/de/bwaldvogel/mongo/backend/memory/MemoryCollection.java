package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.SequencedSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoCollection;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;

public class MemoryCollection extends AbstractSynchronizedMongoCollection<Integer> {

    private final List<Document> documents = new ArrayList<>();
    private final Queue<Integer> emptyPositions = new LinkedList<>();
    private final AtomicLong dataSize = new AtomicLong();
    // only used for capped collections
    private final SequencedSet<Integer> positionInsertionOrder;

    public MemoryCollection(MongoDatabase database, String collectionName,
                            CollectionOptions options, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
        this.positionInsertionOrder = isCapped() ? new LinkedHashSet<>() : null;
    }

    @Override
    protected DocumentWithPosition<Integer> removeOldestDocument() {
        if (positionInsertionOrder.isEmpty()) {
            throw new IllegalStateException("No document to evict from " + getFullName());
        }
        Integer position = positionInsertionOrder.getFirst();
        Document document = removeDocumentInternal(position);
        return new DocumentWithPosition<>(document, position);
    }

    @Override
    protected long getDataSize() {
        return dataSize.get();
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        dataSize.addAndGet(sizeDelta);
    }

    @Override
    protected Integer addDocumentInternal(Document document) {
        Integer position = emptyPositions.poll();
        if (position == null) {
            position = Integer.valueOf(documents.size());
        }

        if (position.intValue() == documents.size()) {
            documents.add(document);
        } else {
            documents.set(position.intValue(), document);
        }

        if (positionInsertionOrder != null) {
            positionInsertionOrder.add(position);
        }
        return position;
    }

    @Override
    protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize, Document fieldSelector) {
        Stream<Document> documentStream = streamDocuments(orderBy);
        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
    }

    private Stream<Document> streamDocuments(Document orderBy) {
        boolean naturalDescending = isNaturalDescending(orderBy);
        if (isCapped()) {
            Stream<Integer> positions = streamPositionsInInsertionOrder(naturalDescending);
            return positions.map(this::getDocument);
        } else {
            Iterable<Document> documents = iterateAllDocuments(naturalDescending);
            return StreamSupport.stream(documents.spliterator(), false);
        }
    }

    private Stream<Integer> streamPositionsInInsertionOrder(boolean isNaturalDescending) {
        if (isNaturalDescending) {
            return this.positionInsertionOrder.reversed().stream();
        } else {
            return this.positionInsertionOrder.stream();
        }
    }

    private Iterable<Document> iterateAllDocuments(boolean naturalDescending) {
        DocumentIterable documentIterable = new DocumentIterable(this.documents);
        if (naturalDescending) {
            return documentIterable.reversed();
        } else {
            return documentIterable;
        }
    }

    @Override
    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    @Override
    public synchronized boolean isEmpty() {
        return documents.isEmpty() || super.isEmpty();
    }

    @Override
    protected Integer findDocumentPosition(Document document) {
        int position = documents.indexOf(document);
        if (position < 0) {
            return null;
        }
        return Integer.valueOf(position);
    }

    @Override
    protected Stream<DocumentWithPosition<Integer>> streamAllDocumentsWithPosition() {
        return IntStream.range(0, documents.size())
            .filter(position -> !emptyPositions.contains(position))
            .mapToObj(index -> new DocumentWithPosition<>(documents.get(index), index));
    }

    @Override
    protected void removeDocument(Integer position) {
        removeDocumentInternal(position);
    }

    private Document removeDocumentInternal(Integer position) {
        Document removed = documents.set(position.intValue(), null);
        emptyPositions.add(position);
        if (positionInsertionOrder != null) {
            positionInsertionOrder.remove(position);
        }
        return removed;
    }

    @Override
    protected Document getDocument(Integer position) {
        return documents.get(position.intValue());
    }

    @Override
    protected void handleUpdate(Integer position, Document oldDocument, Document newDocument) {
        // noop
    }

}
