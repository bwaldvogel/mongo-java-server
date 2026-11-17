package de.bwaldvogel.mongo.backend.h2;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.h2.mvstore.MVMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.bson.Document;

public class H2Collection extends AbstractSynchronizedMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(H2Collection.class);

    static final String CAPPED_ORDER_PREFIX = "cappedOrder.";
    private static final String DATA_SIZE_KEY = "dataSize";
    private static final String CAPPED_SEQUENCE_KEY = "cappedSequence";

    private final MVMap<Object, Document> dataMap;
    private final MVMap<String, Object> metaMap;

    private MVMap<Long, Object> insertionOrder;
    // Rebuilt from insertionOrder when the collection is opened.
    private Map<Object, Long> insertionSequenceByPosition;

    public H2Collection(MongoDatabase database, String collectionName, CollectionOptions options,
                        MVMap<Object, Document> dataMap, MVMap<String, Object> metaMap, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
        this.dataMap = dataMap;
        this.metaMap = metaMap;
        if (isCapped()) {
            this.insertionOrder = dataMap.getStore().openMap(CAPPED_ORDER_PREFIX + getFullName());
            this.insertionSequenceByPosition = new HashMap<>();
            insertionOrder.forEach((sequence, position) -> insertionSequenceByPosition.put(position, sequence));
        }
        if (!this.metaMap.containsKey(DATA_SIZE_KEY)) {
            this.metaMap.put(DATA_SIZE_KEY, Long.valueOf(0));
        } else {
            log.debug("dataSize of {}: {}", getFullName(), getDataSize());
        }
    }

    @Override
    protected DocumentWithPosition<Object> removeOldestDocument() {
        Long sequence = insertionOrder.firstKey();
        if (sequence == null) {
            throw new IllegalStateException("No document to evict from " + getFullName());
        }
        Object position = insertionOrder.remove(sequence);
        insertionSequenceByPosition.remove(position);
        Document document = dataMap.remove(position);
        if (document == null) {
            throw new NoSuchElementException("No document with key " + position);
        }
        return new DocumentWithPosition<>(document, position);
    }

    @Override
    protected long getDataSize() {
        return ((Number) metaMap.get(DATA_SIZE_KEY)).longValue();
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        synchronized (metaMap) {
            Number value = (Number) metaMap.get(DATA_SIZE_KEY);
            Long newValue = Long.valueOf(value.longValue() + sizeDelta);
            metaMap.put(DATA_SIZE_KEY, newValue);
        }
    }

    @Override
    protected Object addDocumentInternal(Document document) {
        final Object key;
        if (getIdField() != null) {
            key = Utils.getSubdocumentValue(document, getIdField());
        } else {
            key = UUID.randomUUID();
        }

        Document previous = dataMap.put(Missing.ofNullable(key), document);
        Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
        if (isCapped()) {
            long sequence = ((Long) metaMap.getOrDefault(CAPPED_SEQUENCE_KEY, 0L)) + 1;
            metaMap.put(CAPPED_SEQUENCE_KEY, sequence);
            Object storedKey = Missing.ofNullable(key);
            insertionOrder.put(sequence, storedKey);
            insertionSequenceByPosition.put(storedKey, sequence);
        }
        return key;
    }

    @Override
    public int count() {
        return dataMap.size();
    }

    @Override
    public boolean isEmpty() {
        return dataMap.isEmpty();
    }

    @Override
    protected Document getDocument(Object position) {
        return dataMap.get(position);
    }

    @Override
    protected void removeDocument(Object position) {
        if (isCapped()) {
            Long sequence = insertionSequenceByPosition.remove(position);
            if (sequence != null) {
                insertionOrder.remove(sequence);
            }
        }
        Document remove = dataMap.remove(position);
        if (remove == null) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
        return dataMap.entrySet().stream()
            .map(entry -> new DocumentWithPosition<>(entry.getValue(), entry.getKey()));
    }

    @Override
    protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize,
                                         Document fieldSelector) {
        Stream<Document> documentStream = getDocumentStream(orderBy);
        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
    }

    private Stream<Document> getDocumentStream(Document orderBy) {
        boolean naturalDescending = isNaturalDescending(orderBy);
        if (isCapped()) {
            Iterable<Long> sequences = () -> naturalDescending
                ? insertionOrder.keyIteratorReverse(null) : insertionOrder.keyIterator(null);
            return StreamSupport.stream(sequences.spliterator(), false).map(insertionOrder::get).map(dataMap::get);
        } else if (naturalDescending) {
            return streamAllDocumentsWithPosition()
                .sorted((o1, o2) -> ValueComparator.desc().compare(o1.position(), o2.position()))
                .map(DocumentWithPosition::document);
        } else {
            return dataMap.values().stream();
        }
    }

    @Override
    protected void handleUpdate(Object position, Document oldDocument, Document newDocument) {
        dataMap.put(Missing.ofNullable(position), newDocument);
    }

}
