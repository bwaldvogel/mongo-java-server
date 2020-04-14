package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

import de.bwaldvogel.mongo.bson.Document;

public class Cursor implements Iterable<Document> {

    private long cursorId;
    private final Queue<Document> documents = new LinkedList<>();
    private final String collectionName;

    /**
     * Creates a cursor. If the documents iterator is null or empty the cursor id will be zero.
     *
     * @param documents      Documents to be stored in the cursor.
     * @param collectionName Name of the collection.
     */
    public Cursor(Iterable<Document> documents, String collectionName) {
        this.collectionName = collectionName;
        if (documents != null) {
            for (Document doc : documents) {
                this.documents.add(doc);
            }
            this.cursorId = isEmpty() ? 0 : Cursor.generateCursorId();
        }
    }

    public Cursor(String collectionName) {
        this(Collections.emptyList(), collectionName);
    }

    public int documentsCount() {
        return documents.size();
    }

    public boolean isEmpty() {
        return documents.isEmpty();
    }

    public long getCursorId() {
        return cursorId;
    }

    public Queue<Document> getDocuments() {
        return documents;
    }

    @Override
    public Iterator<Document> iterator() {
        return documents.iterator();
    }

    public static long generateCursorId() {
        return ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE);
    }

    public String getCollectionName() {
        return collectionName;
    }
}
