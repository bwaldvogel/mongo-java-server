package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

class AbstractMongoBackendTest {

    private MongoBackend backend;
    private CursorFactory cursorFactory = new CursorFactory();

    @BeforeEach
    public void setup() {
        backend = new AbstractMongoBackend() {
            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                return null;
            }
        };
    }

    @Test
    void testGetMore_shouldDeleteCursorIfEmpty() {
        Collection<Document> docs = Arrays.asList(new Document("name", "Joe"), new Document("name", "Mary"),
            new Document("name", "Steve"));
        Cursor cursor = cursorFactory.createInMemoryCursor(docs);
        MongoGetMore getMore = new MongoGetMore(null, null, "testcoll", 3,
            cursor.getCursorId());
        backend.handleGetMore(getMore);
        assertThat(cursorFactory.exists(cursor.getCursorId())).isFalse();
    }

    @Test
    void testHandleKillCursor() {
        Cursor cursor1 = cursorFactory.createInMemoryCursor(Collections.singleton(new Document()));
        Cursor cursor2 = cursorFactory.createInMemoryCursor(Collections.singleton(new Document()));
        assertThat(cursorFactory.exists(cursor1.getCursorId())).isTrue();
        assertThat(cursorFactory.exists(cursor2.getCursorId())).isTrue();
        MongoKillCursors killCursors =
            new MongoKillCursors(null, null, Collections.singletonList(cursor1.getCursorId()));
        backend.handleKillCursors(killCursors);
        assertThat(cursorFactory.exists(cursor1.getCursorId())).isFalse();
        assertThat(cursorFactory.exists(cursor2.getCursorId())).isTrue();
    }

}
