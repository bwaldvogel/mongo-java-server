package de.bwaldvogel.mongo.backend.h2;

import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;

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

    private final MVMap<Object, Document> dataMap;
    private final MVMap<String, Object> metaMap;

    private static final String DATA_SIZE_KEY = "dataSize";

    public H2Collection(MongoDatabase database, String collectionName, CollectionOptions options,
                        MVMap<Object, Document> dataMap, MVMap<String, Object> metaMap, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
        this.dataMap = dataMap;
        this.metaMap = metaMap;
        if (!this.metaMap.containsKey(DATA_SIZE_KEY)) {
            this.metaMap.put(DATA_SIZE_KEY, Long.valueOf(0));
        } else {
            log.debug("dataSize of {}: {}", getFullName(), getDataSize());
        }
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
    protected int getDataSize() {
        Number value = (Number) metaMap.get(DATA_SIZE_KEY);
        return value.intValue();
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
        final Stream<Document> documentStream;
        if (isNaturalDescending(orderBy)) {
            documentStream = streamAllDocumentsWithPosition()
                .sorted((o1, o2) -> ValueComparator.desc().compare(o1.getPosition(), o2.getPosition()))
                .map(DocumentWithPosition::getDocument);
        } else {
            documentStream = dataMap.values().stream();
        }
        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
    }

    @Override
    protected void handleUpdate(Object position, Document oldDocument, Document newDocument) {
        dataMap.put(Missing.ofNullable(position), newDocument);
    }

}
