package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.exception.MongoServerException;

public class CollectionUtilsTest {

    @Test
    void testGetSingleElement() throws Exception {
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
    void testGetLastElement() throws Exception {
        assertThat(CollectionUtils.getLastElement(Collections.singletonList(1))).isEqualTo(1);
        assertThat(CollectionUtils.getLastElement(Arrays.asList(1, 2, 3))).isEqualTo(3);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getLastElement(Collections.emptyList()))
            .withMessage("Given collection must not be empty");
    }

    @Test
    void testGetSingleElement_exceptionSupplier() throws Exception {
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
    void testMultiplyWithOtherElements() throws Exception {
        List<Object> values = Arrays.asList(1, 2);
        List<Object> collection = Arrays.asList("abc", "def", values);

        assertThat(CollectionUtils.multiplyWithOtherElements(collection, Arrays.asList(values)))
            .containsExactly(
                Arrays.asList("abc", "def", 1),
                Arrays.asList("abc", "def", 2)
            );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.multiplyWithOtherElements(collection, Arrays.asList(Arrays.asList(1, 2, 3))))
            .withMessage("Expected [1, 2, 3] to be part of [abc, def, [1, 2]]");

        List<Object> first = Arrays.asList("abc", "xyz", Arrays.asList("L", "M", "S"), Arrays.asList("red", "green", "blue"));
        List<List<Object>> second = Arrays.asList(Arrays.asList("L", "M", "S"), Arrays.asList("red", "green", "blue"));
        assertThat(CollectionUtils.multiplyWithOtherElements(first, second))
            .containsExactly(
                Arrays.asList("abc", "xyz", "L", "red"),
                Arrays.asList("abc", "xyz", "M", "green"),
                Arrays.asList("abc", "xyz", "S", "blue")
            );
    }

    @Test
    void testMultiplyWithOtherElementsWrongSize() throws Exception {
        List<Object> smallValues = Arrays.asList(1, 2);
        List<Object> bigValues = Arrays.asList(1, 2, 3);
        List<Object> collection = Arrays.asList("abc", "def", smallValues, bigValues);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.multiplyWithOtherElements(collection, Arrays.asList(smallValues, bigValues)))
            .withMessage("Expected [1, 2, 3] to be size 2");
    }

    @Test
    void testGetElementAtPosition_list() throws Exception {
        List<String> list = Arrays.asList("a", "b", "c");
        assertThat(CollectionUtils.getElementAtPosition(list, 0)).isEqualTo("a");
        assertThat(CollectionUtils.getElementAtPosition(list, 1)).isEqualTo("b");
        assertThat(CollectionUtils.getElementAtPosition(list, 2)).isEqualTo("c");

        assertThatExceptionOfType(ArrayIndexOutOfBoundsException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(list, 3));

        assertThatExceptionOfType(IndexOutOfBoundsException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(Collections.emptyList(), 0));

        assertThatExceptionOfType(IndexOutOfBoundsException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(Collections.emptyList(), 3));
    }

    @Test
    void testGetElementAtPosition_set() throws Exception {
        Set<String> set = new LinkedHashSet<>();
        set.add("a");
        set.add("b");
        set.add("c");
        assertThat(CollectionUtils.getElementAtPosition(set, 0)).isEqualTo("a");
        assertThat(CollectionUtils.getElementAtPosition(set, 1)).isEqualTo("b");
        assertThat(CollectionUtils.getElementAtPosition(set, 2)).isEqualTo("c");

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(set, 3));

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(Collections.emptySet(), 0));

        assertThatExceptionOfType(NoSuchElementException.class)
            .isThrownBy(() -> CollectionUtils.getElementAtPosition(Collections.emptySet(), 3));
    }

}
