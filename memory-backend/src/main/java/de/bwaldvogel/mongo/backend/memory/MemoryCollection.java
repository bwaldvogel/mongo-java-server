package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;
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
    private final NavigableMap<Long, Integer> insertionOrder;
    private final Map<Integer, Long> insertionSequenceByPosition;
    private long lastSequence;

    public MemoryCollection(MongoDatabase database, String collectionName,
                            CollectionOptions options, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
        this.insertionOrder = isCapped() ? new TreeMap<>() : null;
        this.insertionSequenceByPosition = isCapped() ? new HashMap<>() : null;
    }

    @Override
    protected DocumentWithPosition<Integer> removeOldestDocument() {
        var oldest = insertionOrder.firstEntry();
        if (oldest == null) {
            throw new IllegalStateException("No document to evict from " + getFullName());
        }
        Integer position = oldest.getValue();
        Document document = getDocument(position);
        removeDocument(position);
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

        if (isCapped()) {
            long sequence = ++lastSequence;
            insertionOrder.put(sequence, position);
            insertionSequenceByPosition.put(position, sequence);
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
            return orderedPositions(naturalDescending).values().stream().map(this::getDocument);
        } else {
            Iterable<Document> documents = iterateAllDocuments(naturalDescending);
            return StreamSupport.stream(documents.spliterator(), false);
        }
    }

    @Override
    protected void scanCappedDocuments(Long start, boolean descending, CappedDocumentVisitor visitor) {
        NavigableMap<Long, Integer> positions = orderedPositions(descending);
        if (start != null) {
            positions = positions.tailMap(start, true);
        }
        for (var entry : positions.entrySet()) {
            if (!visitor.visit(entry.getKey(), getDocument(entry.getValue()))) {
                break;
            }
        }
    }

    private NavigableMap<Long, Integer> orderedPositions(boolean descending) {
        return descending ? insertionOrder.descendingMap() : insertionOrder;
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
        if (isCapped()) {
            Long sequence = insertionSequenceByPosition.remove(position);
            if (sequence != null) {
                insertionOrder.remove(sequence);
            }
        }
        documents.set(position.intValue(), null);
        emptyPositions.add(position);
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
