package de.bwaldvogel.mongo.backend.h2;

import java.util.AbstractMap;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Stream;

import org.h2.mvstore.MVMap;
import org.h2.mvstore.tx.Transaction;
import org.h2.mvstore.tx.TransactionMap;
import org.h2.value.VersionedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoCollection;
import de.bwaldvogel.mongo.backend.Assert;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.MongoSession;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.backend.ValueComparator;
import de.bwaldvogel.mongo.bson.Document;

public class H2Collection extends AbstractSynchronizedMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(H2Collection.class);

    private final MVMap<Object, VersionedValue> dataMap;
    private final MVMap<String, Object> metaMap;

    private static final String DATA_SIZE_KEY = "dataSize";

    public H2Collection(MongoDatabase database, String collectionName, CollectionOptions options,
                        MVMap<Object, VersionedValue> dataMap, MVMap<String, Object> metaMap, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
        this.dataMap = dataMap;
        this.metaMap = metaMap;
        if (!this.metaMap.containsKey(DATA_SIZE_KEY)) {
            this.metaMap.put(DATA_SIZE_KEY, 0L);
        } else {
            log.debug("dataSize of {}: {}", getFullName(), getDataSize());
        }
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        synchronized (metaMap) {
            Number value = (Number) metaMap.get(DATA_SIZE_KEY);
            Long newValue = value.longValue() + sizeDelta;
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
        Document previous = (Document) dataMap.put(Missing.ofNullable(key), document);
        Assert.isNull(previous, () -> "Document with key '" + key + "' already existed in " + this + ": " + previous);
        return key;
    }

    @Override
    protected void handleUpdate(Object position, Document oldDocument, Document newDocument) {
        dataMap.put(Missing.ofNullable(position), newDocument);
    }

    @Override
    protected void handleUpdate(Object position, Document oldDocument, Document newDocument, MongoSession mongoSession) {
        if (mongoSession == null) {
            handleUpdate(position, oldDocument, newDocument);
            return;
        }
        Transaction tx = mongoSession.getTransaction();
        TransactionMap<Object, Document> txMap = tx.openMap(dataMap);
        txMap.put(Missing.ofNullable(position), newDocument);
    }

    @Override
    public int count() {
        return dataMap.size();
    }

    @Override
    public boolean isEmpty() {
        return dataMap.isEmpty();
    }

    protected Document getDocument(Object position, MongoSession mongoSession) {
        if (mongoSession == null) {
            return getDocument(position);
        }
        TransactionMap<Object, Document> txMap = mongoSession.getTransaction().openMap(dataMap);
        return txMap.get(position);
    }

    @Override
    protected Document getDocument(Object position) {
        return (Document) dataMap.get(position).getCommittedValue();
    }

    @Override
    protected void removeDocument(Object position) {
        Document remove = (Document) dataMap.remove(position);
        if (remove == null) {
            throw new NoSuchElementException("No document with key " + position);
        }
    }

    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition() {
        return streamAllDocumentsWithPosition(dataMap);
    }

    @Override
    protected Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition(MongoSession mongoSession) {
        if (mongoSession == null) {
            return streamAllDocumentsWithPosition();
        }
        TransactionMap<Object, Document> txMap = mongoSession.getTransaction().openMap(dataMap);
        return streamAllDocumentsWithPosition(txMap.map);
    }

    private Stream<DocumentWithPosition<Object>> streamAllDocumentsWithPosition(AbstractMap<Object, VersionedValue> map) {
        return map.entrySet().stream()
            .map(entry -> new DocumentWithPosition<>((Document) entry.getValue(), entry.getKey()));
    }

    @Override
    protected QueryResult matchDocuments(Document query, Document orderBy, int numberToSkip, int limit, int batchSize,
                                         Document fieldSelector, MongoSession mongoSession) {
        final Stream<Document> documentStream;
        if (isNaturalDescending(orderBy)) {
            documentStream = streamAllDocumentsWithPosition(mongoSession)
                .sorted((o1, o2) -> ValueComparator.desc().compare(o1.getPosition(), o2.getPosition()))
                .map(DocumentWithPosition::getDocument);
        } else {
            documentStream = dataMap.values().stream().map(v -> (Document) v);
        }
        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector, mongoSession);
    }

}
