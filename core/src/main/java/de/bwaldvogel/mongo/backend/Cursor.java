package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import de.bwaldvogel.mongo.bson.Document;

public class Cursor implements Iterable<Document> {

    public static final long EMPTY_CURSOR_ID = 0L;

    private final long cursorId;
    private final Queue<Document> documents = new LinkedList<>();
    private final String collectionName;

    /**
     * Creates a cursor. If the documents iterator is null or empty the cursor id will be zero.
     *
     * @param documents      Documents to be stored in the cursor.
     * @param collectionName Name of the collection.
     */
    public Cursor(Iterable<Document> documents, String collectionName, long cursorId) {
        this.cursorId = cursorId;
        this.collectionName = collectionName;
        if (documents != null) {
            for (Document document : documents) {
                this.documents.add(document);
            }
        }
    }

    public Cursor(String collectionName) {
        this(Collections.emptyList(), collectionName, EMPTY_CURSOR_ID);
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

    public String getCollectionName() {
        return collectionName;
    }
}
