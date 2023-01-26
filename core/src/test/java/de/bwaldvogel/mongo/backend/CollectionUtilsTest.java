package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

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
        assertThat(CollectionUtils.getSingleElement(List.of(1))).isEqualTo(1);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Collections.emptyList()))
            .withMessage("Expected one element but got zero");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(List.of(1, 2)))
            .withMessage("Expected one element but got at least two");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(List.of(1, 2, 3)))
            .withMessage("Expected one element but got at least two");
    }

    @Test
    void testGetLastElement() throws Exception {
        assertThat(CollectionUtils.getLastElement(List.of(1))).isEqualTo(1);
        assertThat(CollectionUtils.getLastElement(List.of(1, 2, 3))).isEqualTo(3);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.getLastElement(Collections.emptyList()))
            .withMessage("Given collection must not be empty");
    }

    @Test
    void testGetSingleElement_exceptionSupplier() throws Exception {
        Supplier<RuntimeException> exceptionSupplier = () -> new MongoServerException("too many elements");

        assertThat(CollectionUtils.getSingleElement(List.of(1), exceptionSupplier)).isEqualTo(1);

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(Collections.emptyList(), exceptionSupplier))
            .withMessage("too many elements");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> CollectionUtils.getSingleElement(List.of(1, 2), exceptionSupplier))
            .withMessage("too many elements");
    }

    @Test
    void testMultiplyWithOtherElements() throws Exception {
        List<Object> values = List.of(1, 2);
        List<Object> collection = List.of("abc", "def", values);

        assertThat(CollectionUtils.multiplyWithOtherElements(collection, List.of(values)))
            .containsExactly(
                List.of("abc", "def", 1),
                List.of("abc", "def", 2)
            );

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.multiplyWithOtherElements(collection, List.of(List.of(1, 2, 3))))
            .withMessage("Expected [1, 2, 3] to be part of [abc, def, [1, 2]]");

        List<Object> first = List.of("abc", "xyz", List.of("L", "M", "S"), List.of("red", "green", "blue"));
        List<List<Object>> second = List.of(List.of("L", "M", "S"), List.of("red", "green", "blue"));
        assertThat(CollectionUtils.multiplyWithOtherElements(first, second))
            .containsExactly(
                List.of("abc", "xyz", "L", "red"),
                List.of("abc", "xyz", "M", "green"),
                List.of("abc", "xyz", "S", "blue")
            );
    }

    @Test
    void testMultiplyWithOtherElementsWrongSize() throws Exception {
        List<Object> smallValues = List.of(1, 2);
        List<Object> bigValues = List.of(1, 2, 3);
        List<Object> collection = List.of("abc", "def", smallValues, bigValues);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> CollectionUtils.multiplyWithOtherElements(collection, List.of(smallValues, bigValues)))
            .withMessage("Expected [1, 2, 3] to be size 2");
    }

    @Test
    void testGetElementAtPosition_list() throws Exception {
        List<String> list = List.of("a", "b", "c");
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
