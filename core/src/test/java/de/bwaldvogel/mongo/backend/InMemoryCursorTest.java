package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
    void testGetCursorId() {
        Cursor cursor = new InMemoryCursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.getCursorId()).isEqualTo(1);
    }

    @Test
    void testTakeDocuments() {
        List<Document> documents = Arrays.asList(new Document("name", "Joe"), new Document("name", "Mary"));

        InMemoryCursor cursor1 = new InMemoryCursor(1L, documents);
        assertThat(cursor1.takeDocuments(10)).containsExactlyElementsOf(documents);

        InMemoryCursor cursor2 = new InMemoryCursor(1L, documents);
        assertThat(cursor2.takeDocuments(1)).containsExactly(documents.get(0));
        assertThat(cursor2.takeDocuments(1)).containsExactly(documents.get(1));
        assertThat(cursor2.takeDocuments(1)).isEmpty();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> cursor2.takeDocuments(0))
            .withMessage("Illegal number to return: 0");
    }

    @Test
    void testToString() throws Exception {
        assertThat(new InMemoryCursor(123L, Collections.singleton(new Document())))
            .hasToString("InMemoryCursor(id: 123)");
    }

}
