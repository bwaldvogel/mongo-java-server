package de.bwaldvogel.mongo.backend;

import java.util.Collections;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.MongoCursorNotFoundException;
import com.mongodb.client.MongoCursor;

public abstract class AbstractFakeBackendTest extends AbstractBackendTest {

    @Test
    public void testCursor_iteratingACursorThatNoLongerExists() {
        int expectedCount = 20;
        for (int i = 0; i < expectedCount; i++) {
            collection.insertOne(new Document("name", "testUser1"));
        }
        MongoCursor<Document> cursor = collection.find().batchSize(1).cursor();
        cursor.next();
        killCursors(Collections.singletonList(cursor.getServerCursor().getId()));
        Assertions.assertThrows(MongoCursorNotFoundException.class, cursor::next);
    }

}
