package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        List<Document> documents = IntStream.range(0, 22)
            .mapToObj(idx -> new Document("number", idx + 1))
            .collect(Collectors.toList());

        InMemoryCursor cursor = new InMemoryCursor(1L, documents);
        assertThat(cursor.takeDocuments(1)).containsExactly(documents.get(0));
        assertThat(cursor.takeDocuments(2)).containsExactly(documents.get(1), documents.get(2));
        assertThat(cursor.takeDocuments(1)).containsExactly(documents.get(3));
        assertThat(cursor.takeDocuments(6)).containsExactlyElementsOf(documents.subList(4, 10));
        assertThat(cursor.takeDocuments(10)).containsExactlyElementsOf(documents.subList(10, 20));
        assertThat(cursor.takeDocuments(10)).containsExactly(documents.get(20), documents.get(21));
        assertThat(cursor.takeDocuments(10)).isEmpty();

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> cursor.takeDocuments(0))
            .withMessage("Illegal number to return: 0");
    }

    @Test
    void testToString() throws Exception {
        assertThat(new InMemoryCursor(123L, Collections.singleton(new Document())))
            .hasToString("InMemoryCursor(id: 123)");
    }

}
