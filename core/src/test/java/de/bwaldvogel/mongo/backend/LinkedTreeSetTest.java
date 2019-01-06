package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

public class LinkedTreeSetTest {

    @Test
    public void testAddContainsRemove() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set.add(1)).isTrue();
        assertThat(set.add(-0.0)).isTrue();
        assertThat(set.add(0)).isFalse();
        assertThat(set.add(null)).isTrue();
        assertThat(set.add("abc")).isTrue();

        assertThat(set).containsExactly(1, -0.0, null, "abc");

        assertThat(set.remove("xyz")).isFalse();
        assertThat(set.remove(null)).isTrue();

        assertThat(set).containsExactly(1, -0.0, "abc");

        assertThat(set.remove(0.0)).isTrue();

        assertThat(set).containsExactly(1, "abc");

        assertThat(set.retainAll(Arrays.asList("abc", "xyz"))).isTrue();

        assertThat(set).containsExactly("abc");

        assertThat(set.retainAll(Arrays.asList("abc", "xyz"))).isFalse();
    }

    @Test
    public void testSize() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set).isEmpty();
        assertThat(set).hasSize(0);

        set.add("abc");
        set.add("xyz");

        assertThat(set).isNotEmpty();
        assertThat(set).hasSize(2);
    }

    @Test
    public void testToString() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set).hasToString("[]");

        set.add("abc");
        set.add("def");

        assertThat(set).hasToString("[abc, def]");
    }

    @Test
    public void testEqualsAndHashCode() throws Exception {
        Set<Object> set1 = new LinkedTreeSet<>();
        Set<Object> set2 = new LinkedTreeSet<>();

        assertThat(set1).hasSameHashCodeAs(set2);
        assertThat(set1).isEqualTo(set2);

        set1.add("abc");

        assertThat(set1).isNotEqualTo(set2);

        set2.add("abc");

        assertThat(set1).hasSameHashCodeAs(set2);
        assertThat(set1).isEqualTo(set2);

        set1.add(1);
        set1.add(2);

        set2.add(2);
        set2.add(1);

        assertThat(set1).isNotEqualTo(set2);
    }

    @Test
    public void testIterator() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        set.add(null);
        set.add("abc");

        Iterator<Object> iterator = set.iterator();
        assertThat(iterator.hasNext()).isTrue();

        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(iterator::remove);

        assertThat(set).containsExactly(null, "abc");

        assertThat(iterator.next()).isNull();
        iterator.remove();

        assertThat(set).containsExactly("abc");

        assertThat(iterator.hasNext()).isTrue();
        assertThat(iterator.next()).isEqualTo("abc");
        iterator.remove();

        assertThat(set).isEmpty();

        assertThat(iterator.hasNext()).isFalse();
    }

}