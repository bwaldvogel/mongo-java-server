package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.backend.aggregation.Aggregation;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CursorNotFoundException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CursorFactory {

    final ConcurrentMap<Long, Cursor> cursors = new ConcurrentHashMap<>();
    final AtomicLong cursorIdCounter = new AtomicLong();

    public synchronized long getCursorId() {
        return cursorIdCounter.incrementAndGet();
    }

    public synchronized InMemoryCursor createInMemoryCursor(Collection<Document> remainingDocuments) {
        return createCursor(new InMemoryCursor(getCursorId(), new ArrayList<>(remainingDocuments)));
    }

    public Cursor getCursor(long cursorId) {
        if (!cursors.containsKey(cursorId)) {
            throw new CursorNotFoundException(String.format("Cursor id %d does not exists", cursorId));
        }
        return cursors.get(cursorId);
    }

    public boolean exists(long cursorId) {
        return cursors.containsKey(cursorId);
    }

    public synchronized void remove(long cursorId) {
        cursors.remove(cursorId);
    }

    public TailableCursor createTailableCursor(Aggregation aggregation, MongoBackendClock backendClock) {
        return createCursor(
            new TailableCursor(getCursorId(), aggregation, Utils.getResumeToken(backendClock.get()))
        );
    }

    public TailableCursor createTailableCursor(Aggregation aggregation, long resumeToken) {
        return createCursor(new TailableCursor(getCursorId(), aggregation, resumeToken));
    }

    private <T extends Cursor> T createCursor(T cursor) {
        Cursor previousValue = cursors.put(cursor.getCursorId(), cursor);
        Assert.isNull(previousValue);
        return cursor;
    }
}
