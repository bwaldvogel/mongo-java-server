package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

public class AssertTest {

    @Test
    public void testIsEmpty() throws Exception {
        Assert.isEmpty(Collections.emptyList());
        Assert.isEmpty(Collections.emptySet());
        Assert.isEmpty(new ArrayList<>());

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isEmpty(Arrays.asList("a", "b", "c")))
            .withMessage("Expected [a, b, c] to be empty");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isEmpty(Arrays.asList("a", "b", "c"), () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testNotEmpty() throws Exception {
        Assert.notEmpty(Arrays.asList("a", "b", "c"));

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notEmpty(Collections.emptySet()))
            .withMessage("Given collection must not be empty");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notEmpty(Collections.emptySet(), () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testHasSize() throws Exception {
        Assert.hasSize(Arrays.asList("a", "b", "c"), 3);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.hasSize(Collections.emptySet(), 1))
            .withMessage("Expected [] to have size 1 but got 0 elements");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.hasSize(Collections.emptySet(), 1, () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testEquals() throws Exception {
        Assert.equals(null, null);
        Assert.equals("a", "a");
        Assert.equals(123, 123);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.equals("a", "b"))
            .withMessage("Expected 'a' to be equal to 'b'");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.equals(1, 2))
            .withMessage("Expected 1 to be equal to 2");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.equals("a", "b", () -> "some message"))
            .withMessage("some message");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.equals(1, 2, () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testIsTrue() throws Exception {
        Assert.isTrue(true, () -> "");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isTrue(false, () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testIsFalse() throws Exception {
        Assert.isFalse(false, () -> "");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isFalse(true, () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testIsNull() throws Exception {
        Assert.isNull(null);

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isNull("abc"))
            .withMessage("Given value 'abc' must not be null");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.isNull(new Object(), () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testNotNull() throws Exception {
        Assert.notNull("");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notNull(null))
            .withMessage("Given value must not be null");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notNull(null, () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testStartsWith() throws Exception {
        Assert.startsWith("abc", "ab");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.startsWith("abc", "b"))
            .withMessage("'abc' must start with 'b'");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.startsWith("abc", "b", () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testDoesNotStartWith() throws Exception {
        Assert.doesNotStartWith("a", "b");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.doesNotStartWith("abc", "ab"))
            .withMessage("'abc' must not start with 'ab'");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.doesNotStartWith("abc", "ab", () -> "some message"))
            .withMessage("some message");
    }

    @Test
    public void testNotNullOrEmpty() throws Exception {
        Assert.notNullOrEmpty("a");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notNullOrEmpty(null))
            .withMessage("Given string 'null' must not be null or empty");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notNullOrEmpty(""))
            .withMessage("Given string '' must not be null or empty");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> Assert.notNullOrEmpty("", () -> "some message"))
            .withMessage("some message");
    }

}
