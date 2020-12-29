package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.backend.AbstractSynchronizedMongoCollection;
import de.bwaldvogel.mongo.backend.CollectionOptions;
import de.bwaldvogel.mongo.backend.CursorRegistry;
import de.bwaldvogel.mongo.backend.DocumentWithPosition;
import de.bwaldvogel.mongo.backend.MongoSession;
import de.bwaldvogel.mongo.backend.QueryResult;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.InvalidOptionsException;
import de.bwaldvogel.mongo.exception.MongoServerException;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class MemoryCollection extends AbstractSynchronizedMongoCollection<Integer> {

    private final List<Document> documents = new ArrayList<>();
    private final Queue<Integer> emptyPositions = new LinkedList<>();
    private final AtomicInteger dataSize = new AtomicInteger();

    public MemoryCollection(MongoDatabase database, String collectionName,
                            CollectionOptions options, CursorRegistry cursorRegistry) {
        super(database, collectionName, options, cursorRegistry);
    }

    @Override
    protected void updateDataSize(int sizeDelta) {
        dataSize.addAndGet(sizeDelta);
    }

    @Override
    protected int getDataSize() {
        return dataSize.get();
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
        return position;
    }

    @Override
    protected QueryResult matchDocuments(
        Document query, Document orderBy, int numberToSkip, int limit, int batchSize, Document fieldSelector,
        MongoSession mongoSession) {
        Iterable<Document> documents = iterateAllDocuments(orderBy);
        Stream<Document> documentStream = StreamSupport.stream(documents.spliterator(), false);
        return matchDocumentsFromStream(documentStream, query, orderBy, numberToSkip, limit, batchSize, fieldSelector);
    }

    private Iterable<Document> iterateAllDocuments(Document orderBy) {
        DocumentIterable documentIterable = new DocumentIterable(this.documents);
        if (isNaturalDescending(orderBy)) {
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
        documents.set(position.intValue(), null);
        emptyPositions.add(position);
    }

    @Override
    protected Document getDocument(Integer position) {
        return documents.get(position.intValue());
    }

    @Override
    protected void handleUpdate(Integer position, Document oldDocument, Document newDocument) {
        Document doc = documents.get(position);
        for (String key : newDocument.keySet()) {
            if (key.contains(".")) {
                throw new MongoServerException(
                    "illegal field name. must not happen as it must be caught by the driver");
            }
            doc.put(key, newDocument.get(key));
        }
    }

    @Override
    protected void handleUpdate(Integer position, Document oldDocument, Document newDocument, MongoSession mongoSession) {
        handleUpdate(position, oldDocument, newDocument);
    }

}
