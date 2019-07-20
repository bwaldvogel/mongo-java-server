package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import org.junit.Test;

import de.bwaldvogel.mongo.exception.MongoServerException;

public class CollectionUtilsTest {

    @Test
    public void testGetSingleElement() throws Exception {
        assertThat(CollectionUtils.getSingleElement(Collections.singletonList(1))).isEqualTo(1);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Collections.emptyList()))
            .withMessage("Expected one element but got zero");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Arrays.asList(1, 2)))
            .withMessage("Expected one element but got at least two");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Arrays.asList(1, 2, 3)))
            .withMessage("Expected one element but got at least two");
    }

    @Test
    public void testGetSingleElement_exceptionSupplier() throws Exception {
        Supplier<RuntimeException> exceptionSupplier = () -> new MongoServerException("too many elements");

        assertThat(CollectionUtils.getSingleElement(Collections.singletonList(1), exceptionSupplier)).isEqualTo(1);

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Collections.emptyList(), exceptionSupplier))
            .withMessage("too many elements");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Arrays.asList(1, 2), exceptionSupplier))
            .withMessage("too many elements");
    }

}
