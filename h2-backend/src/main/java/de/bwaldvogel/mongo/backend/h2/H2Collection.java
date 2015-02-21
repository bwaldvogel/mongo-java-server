package de.bwaldvogel.mongo.backend.h2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.UUID;

import org.bson.BSONObject;
import org.h2.mvstore.MVMap;
import org.h2.mvstore.MVStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class H2Collection extends AbstractMongoCollection<Object> {

    private static final Logger log = LoggerFactory.getLogger(H2Collection.class);

    private MVMap<Object, BSONObject> mvMap;

    public H2Collection(String databaseName, String collectionName, String idField, MVMap<Object, BSONObject> mvMap) {
        super(databaseName, collectionName, idField);
        this.mvMap = mvMap;
    }

    @Override
    protected Object addDocumentInternal(BSONObject document) {
        final Object key;
        if (idField != null) {
            key = Utils.getSubdocumentValue(document, idField);
        } else {
            key = UUID.randomUUID();
        }

        BSONObject previous = mvMap.put(key, document);
        if (previous != null) {
            throw new IllegalArgumentException("Document with key '" + key + "' already existed in " + this + ": "
                    + previous);
        }
        return key;
    }

    @Override
    public int count() {
        return mvMap.size();
    }

    @Override
    protected BSONObject getDocument(Object key) {
        return mvMap.get(key);
    }

    @Override
    protected void removeDocumentWithKey(Object key) {
        BSONObject remove = mvMap.remove(key);
        if (remove == null) {
            throw new NoSuchElementException("No document with key " + key);
        }
    }

    @Override
    protected Object findDocument(BSONObject document) {
        for (Entry<Object, BSONObject> entry : mvMap.entrySet()) {
            if (entry.getValue().equals(document)) {
                return entry.getKey();
            }
        }
        return null;
    }


    @Override
    protected Iterable<BSONObject> matchDocuments(BSONObject query, Iterable<Object> keys, BSONObject orderBy, int numberToSkip, int numberToReturn) throws MongoServerException {

        List<BSONObject> matchedDocuments = new ArrayList<BSONObject>();

        for (Object key : keys) {
            BSONObject document = getDocument(key);
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // keep it as is
                } else if (sortValue == -1) {
                    Collections.reverse(matchedDocuments);
                } else {
                    throw new IllegalArgumentException("Illegal sort value: " + sortValue);
                }
            } else {
                Collections.sort(matchedDocuments, new DocumentComparator(orderBy));
            }
        }

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
        List<BSONObject> matchedDocuments = new ArrayList<BSONObject>();

        for(BSONObject document : mvMap.values()) {
            if (documentMatchesQuery(document, query)) {
                matchedDocuments.add(document);
            }
        }

        if (orderBy != null && !orderBy.keySet().isEmpty()) {
            if (orderBy.keySet().iterator().next().equals("$natural")) {
                int sortValue = ((Integer) orderBy.get("$natural")).intValue();
                if (sortValue == 1) {
                    // already sorted
                } else if (sortValue == -1) {
                    Collections.reverse(matchedDocuments);
                }
            } else {
                Collections.sort(matchedDocuments, new DocumentComparator(orderBy));
            }
        }

        if (numberToSkip > 0) {
            matchedDocuments = matchedDocuments.subList(numberToSkip, matchedDocuments.size());
        }

        if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
            matchedDocuments = matchedDocuments.subList(0, numberToReturn);
        }

        return matchedDocuments;
    }

    @Override
    protected int getRecordCount() {
        return count();
    }

    @Override
    protected int getDeletedCount() {
        return 0;
    }

    @Override
    public void drop() {
        log.debug("dropping {}", this);
        MVStore mvStore = mvMap.getStore();
        mvStore.removeMap(mvMap);
    }

}
