package de.bwaldvogel.mongo.backend.memory;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.NoSuchElementException;

import org.junit.Test;

public class DocumentIterableTest {

    @Test
    public void testIterateEmptyList() {
        DocumentIterable documentIterable = new DocumentIterable(Collections.emptyList());
        assertThat(documentIterable.iterator().hasNext()).isFalse();

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> documentIterable.iterator().next());
    }

}
