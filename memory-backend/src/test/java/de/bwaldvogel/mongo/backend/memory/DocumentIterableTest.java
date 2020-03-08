package de.bwaldvogel.mongo.backend.memory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Collections;
import java.util.NoSuchElementException;

import org.junit.jupiter.api.Test;

public class DocumentIterableTest {

    @Test
    void testIterateEmptyList() {
        DocumentIterable documentIterable = new DocumentIterable(Collections.emptyList());
        assertThat(documentIterable.iterator().hasNext()).isFalse();

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> documentIterable.iterator().next());
    }

}
