package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;

class InMemoryCursorTest {

    @Test
    void testDocumentsCount() {
        InMemoryCursor cursor = new InMemoryCursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.documentsCount()).isEqualTo(1);
    }

    @Test
    void testIsEmpty() {
        InMemoryCursor cursor = new InMemoryCursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.isEmpty()).isFalse();
    }

    @Test
    void test_getCursorId() {
        Cursor cursor = new InMemoryCursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.getCursorId()).isEqualTo(1);
    }

    @Test
    void testPollDocument() {
        Collection<Document> documents = Arrays.asList(new Document("name", "Joe"), new Document("name", "Mary"));
        Cursor cursor = new InMemoryCursor(1L, documents);
        for (Document document : documents) {
            assertThat(cursor.pollDocument()).isEqualTo(document);
        }
    }

    @Test
    void testToString() throws Exception {
        assertThat(new InMemoryCursor(123L, Collections.singleton(new Document())))
            .hasToString("InMemoryCursor(id: 123)");
    }

}
