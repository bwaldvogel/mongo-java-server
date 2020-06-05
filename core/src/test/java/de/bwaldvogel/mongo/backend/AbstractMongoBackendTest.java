package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.MongoDatabase;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.CursorNotFoundException;
import de.bwaldvogel.mongo.wire.message.MongoGetMore;
import de.bwaldvogel.mongo.wire.message.MongoKillCursors;

class AbstractMongoBackendTest {

    private MongoBackend backend;
    private CursorRegistry cursorRegistry;

    @BeforeEach
    public void setup() {
        backend = new AbstractMongoBackend() {

            {
                AbstractMongoBackendTest.this.cursorRegistry = getCursorRegistry();
            }

            @Override
            protected MongoDatabase openOrCreateDatabase(String databaseName) {
                return null;
            }
        };
    }

    @Test
    void testGetMore_shouldDeleteCursorIfEmpty() {
        List<Document> documents = Arrays.asList(
            new Document("name", "Joe"),
            new Document("name", "Mary"),
            new Document("name", "Steve"));
        Cursor cursor = new InMemoryCursor(cursorRegistry.generateCursorId(), documents);
        cursorRegistry.add(cursor);
        MongoGetMore getMore = new MongoGetMore(null, null, "testcoll", 3,
            cursor.getId());
        backend.handleGetMore(getMore.getCursorId(), getMore.getNumberToReturn());

        assertThatExceptionOfType(CursorNotFoundException.class)
            .isThrownBy(() -> cursorRegistry.getCursor(cursor.getId()))
            .withMessage("[Error 43] Cursor id 1 does not exists");
    }

    @Test
    void testHandleKillCursor() {
        InMemoryCursor cursor1 = new InMemoryCursor(cursorRegistry.generateCursorId(), Collections.singletonList(new Document()));
        InMemoryCursor cursor2 = new InMemoryCursor(cursorRegistry.generateCursorId(), Collections.singletonList(new Document()));
        cursorRegistry.add(cursor1);
        cursorRegistry.add(cursor2);
        assertThat(cursorRegistry.getCursor(cursor1.getId())).isNotNull();
        assertThat(cursorRegistry.getCursor(cursor2.getId())).isNotNull();

        MongoKillCursors killCursors = new MongoKillCursors(null, null, Collections.singletonList(cursor1.getId()));
        backend.handleKillCursors(killCursors);

        assertThatExceptionOfType(CursorNotFoundException.class)
            .isThrownBy(() -> cursorRegistry.getCursor(cursor1.getId()))
            .withMessage("[Error 43] Cursor id 1 does not exists");

        assertThat(cursorRegistry.getCursor(cursor2.getId())).isNotNull();
    }

}
