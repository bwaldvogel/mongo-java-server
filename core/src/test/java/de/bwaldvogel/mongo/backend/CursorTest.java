package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;

class CursorTest {

    @Test
    void documentsCount() {
        Cursor cursor = Cursor.empty();
        assertThat(cursor.documentsCount()).isEqualTo(0);
        cursor = new Cursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.documentsCount()).isEqualTo(1);
    }

    @Test
    void isEmpty() {
        Cursor cursor = Cursor.empty();
        assertThat(cursor.isEmpty()).isTrue();
        cursor = new Cursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.isEmpty()).isFalse();
    }

    @Test
    void getCursorId_emptyCursorShouldHaveIdEqualsToZero() {
        Cursor cursor = Cursor.empty();
        assertThat(cursor.getCursorId()).isEqualTo(0);
    }

    @Test
    void getCursorId_ShouldHaveIdEqualsGreaterThanZero() {
        Cursor cursor = new Cursor(1L, Collections.singleton(new Document()));
        assertThat(cursor.getCursorId()).isGreaterThan(0);
    }

    @Test
    void testPollDocument() {
        Collection<Document> docs = Arrays.asList(new Document("name", "Joe"), new Document("name", "Mary"));
        Cursor cursor = new Cursor(1L, docs);
        for (Document doc : docs) {
            assertThat(cursor.pollDocument()).isEqualTo(doc);
        }
    }

    @Test
    void testToString() throws Exception {
        assertThat(Cursor.empty()).hasToString("Cursor(id: 0)");
        assertThat(new Cursor(123L, Collections.singleton(new Document()))).hasToString("Cursor(id: 123)");
    }

}
