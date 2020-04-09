package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

class EmptyCursorTest {

    @Test
    void testIsEmpty() {
        EmptyCursor cursor = EmptyCursor.get();
        assertThat(cursor.isEmpty()).isTrue();
    }

    @Test
    void testGetCursorId() {
        Cursor cursor = EmptyCursor.get();
        assertThat(cursor.getCursorId()).isEqualTo(0);
    }

    @Test
    void testTakeDocuments() {
        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> EmptyCursor.get().takeDocuments(1));
    }

    @Test
    void testToString() throws Exception {
        assertThat(EmptyCursor.get()).hasToString("EmptyCursor()");
    }

}
