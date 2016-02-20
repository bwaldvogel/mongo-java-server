package de.bwaldvogel.mongo.backend.memory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class MemoryCollection extends AbstractMongoCollection<Integer> {

    private static final Logger log = LoggerFactory.getLogger(MemoryCollection.class);

    private List<BSONObject> documents = new ArrayList<>();
    private Queue<Integer> emptyPositions = new LinkedList<>();
    private AtomicLong dataSize = new AtomicLong();

    public MemoryCollection(String databaseName, String collectionName, String idField) {
        super(databaseName, collectionName, idField);
    }

    @Override
    protected void updateDataSize(long sizeDelta) {
        dataSize.addAndGet(sizeDelta);
    }

    @Override
    protected long getDataSize() {
        return dataSize.get();
    }

    @Override
    protected Integer addDocumentInternal(BSONObject document) {
        Integer pos = emptyPositions.poll();
        if (pos == null) {
            pos = Integer.valueOf(documents.size());
        }

        if (pos.intValue() == documents.size()) {
            documents.add(document);
        } else {
            documents.set(pos.intValue(), document);
        }
        return pos;
    }

    @Override
    protected Iterable<BSONObject> matchDocuments(BSONObject query, Iterable<Integer> keys, BSONObject orderBy, int numberToSkip, int numberToReturn) throws MongoServerException {

        List<BSONObject> matchedDocuments = new ArrayList<>();

        for (Integer key : keys) {
            BSONObject document = getDocument(key);
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        sortDocumentsInMemory(matchedDocuments, orderBy);

        if (numberToSkip > 0) {
            matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    @Override
    protected Iterable<BSONObject> matchDocuments(BSONObject query, BSONObject orderBy, int numberToSkip,
            int numberToReturn) throws MongoServerException {
        List<BSONObject> matchedDocuments = new ArrayList<>();

        boolean ascending = true;
        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == -1) {
                    ascending = false;
                }
            }
        }

        for (BSONObject document : iterateAllDocuments(ascending)) {
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                // already sorted
            } else {
                Collections.sort(matchedDocuments, new DocumentComparator(orderBy));
            }
        }

        if (numberToSkip > 0) {
            if (numberToSkip < matchedDocuments.size()) {
                matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
            } else {
                return Collections.emptyList();
            }
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    private static abstract class AbstractDocumentIterator implements Iterator<BSONObject> {

        protected int pos;
        protected final List<BSONObject> documents;
        protected BSONObject current;

        protected AbstractDocumentIterator(List<BSONObject> documents, int pos) {
            this.documents = documents;
            this.pos = pos;
        }

        protected abstract BSONObject getNext();

        @Override
        public boolean hasNext() {
            if (current == null) {
                current = getNext();
            }
            return (current != null);
        }

        @Override
        public BSONObject next() {
            BSONObject document = current;
            current = getNext();
            return document;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    private static class DocumentIterator extends AbstractDocumentIterator {

        protected DocumentIterator(List<BSONObject> documents) {
            super(documents, 0);
        }

        @Override
        protected BSONObject getNext() {
            while (pos < documents.size()) {
                BSONObject document = documents.get(pos++);
                if (document != null) {
                    return document;
                }
            }
            return null;
        }

    }

    private static class ReverseDocumentIterator extends AbstractDocumentIterator {

        protected ReverseDocumentIterator(List<BSONObject> documents) {
            super(documents, documents.size() - 1);
        }

        @Override
        protected BSONObject getNext() {
            while (pos >= 0) {
                BSONObject document = documents.get(pos--);
                if (document != null) {
                    return document;
                }
            }
            return null;
        }

    }

    private static class DocumentIterable implements Iterable<BSONObject> {

        private List<BSONObject> documents;

        public DocumentIterable(List<BSONObject> documents) {
            this.documents = documents;
        }

        @Override
        public Iterator<BSONObject> iterator() {
            return new DocumentIterator(documents);
        }

    }

    private static class ReverseDocumentIterable implements Iterable<BSONObject> {

        private List<BSONObject> documents;

        public ReverseDocumentIterable(List<BSONObject> documents) {
            this.documents = documents;
        }

        @Override
        public Iterator<BSONObject> iterator() {
            return new ReverseDocumentIterator(documents);
        }

    }

    private Iterable<BSONObject> iterateAllDocuments(boolean ascending) {
        if (ascending) {
            return new DocumentIterable(documents);
        } else {
            return new ReverseDocumentIterable(documents);
        }
    }

    @Override
    public synchronized int count() {
        return documents.size() - emptyPositions.size();
    }

    @Override
    protected Integer findDocument(BSONObject document) {
        int position = documents.indexOf(document);
        if (position < 0) {
            return null;
        }
        return Integer.valueOf(position);
    }

    @Override
    protected int getRecordCount() {
        return documents.size();
    }

    @Override
    protected int getDeletedCount() {
        return emptyPositions.size();
    }

    @Override
    protected void removeDocumentWithKey(Integer position) {
        documents.set(position.intValue(), null);
        emptyPositions.add(position);
    }

    @Override
    protected BSONObject getDocument(Integer position) {
        return documents.get(position.intValue());
    }

    @Override
    public void drop() {
        log.debug("dropping {}", this);
    }

}
