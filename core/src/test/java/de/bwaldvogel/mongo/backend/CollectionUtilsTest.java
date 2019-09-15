package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    public void testGetLastElement() throws Exception {
        assertThat(CollectionUtils.getLastElement(Collections.singletonList(1))).isEqualTo(1);
        assertThat(CollectionUtils.getLastElement(Arrays.asList(1, 2, 3))).isEqualTo(3);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getLastElement(Collections.emptyList()))
            .withMessage("Given collection must not be empty");
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

    @Test
    public void testMultiplyWithOtherElements() throws Exception {
        List<Object> values = Arrays.asList(1, 2);
        List<Object> collection = Arrays.asList("abc", "def", values);

        assertThat(CollectionUtils.multiplyWithOtherElements(collection, values))
            .containsExactly(
                Arrays.asList("abc", "def", 1),
                Arrays.asList("abc", "def", 2)
            );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.multiplyWithOtherElements(collection, Arrays.asList(1, 2, 3)))
            .withMessage("Expected [1, 2, 3] to be part of [abc, def, [1, 2]]");
    }

}
