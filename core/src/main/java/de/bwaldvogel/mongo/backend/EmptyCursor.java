package de.bwaldvogel.mongo.backend;

import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import de.bwaldvogel.mongo.bson.Document;

public class EmptyCursor extends AbstractCursor {

    private static final long EMPTY_CURSOR_ID = 0L;

    private static final EmptyCursor INSTANCE = new EmptyCursor();

    private EmptyCursor() {
        super(EMPTY_CURSOR_ID, Collections.emptyList());
    }

    public static EmptyCursor get() {
        return INSTANCE;
    }

    @Override
    public List<Document> takeDocuments(int numberToReturn) {
        throw new NoSuchElementException();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()+"()";
    }
}
