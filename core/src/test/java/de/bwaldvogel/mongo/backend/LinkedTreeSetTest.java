package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class LinkedTreeSetTest {

    @Test
    void testAddContainsRemove() throws Exception {
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

        assertThat(set.retainAll(List.of("abc", "xyz"))).isTrue();

        assertThat(set).containsExactly("abc");

        assertThat(set.retainAll(List.of("abc", "xyz"))).isFalse();
    }

    @Test
    void testSize() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set).isEmpty();
        assertThat(set).hasSize(0);

        set.add("abc");
        set.add("xyz");

        assertThat(set).isNotEmpty();
        assertThat(set).hasSize(2);
    }

    @Test
    void testToString() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set).hasToString("[]");

        set.add("abc");
        set.add("def");

        assertThat(set).hasToString("[abc, def]");
    }

    @Test
    void testEqualsAndHashCode() throws Exception {
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
    void testIterator() throws Exception {
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

    @Test
    void testListValues() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();
        assertThat(set.add(List.of(1, 2, 3))).isTrue();
        assertThat(set.add(List.of(2, 3))).isTrue();
        assertThat(set.add(List.of(1))).isTrue();

        assertThat(set).hasSize(3);

        assertThat(set.remove(List.of(1, 2))).isFalse();
        assertThat(set.remove(List.of(1, 2, 3.0))).isTrue();

        assertThat(set).containsExactlyInAnyOrder(List.of(2, 3), List.of(1));
    }

    @Test
    void testRemoveAll() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();

        assertThat(set.removeAll(List.of(2, -0.0))).isFalse();

        set.add(1.0);
        set.add(0);
        set.add(2);

        assertThat(set.removeAll(List.of(3, -0.0))).isTrue();
        assertThat(set.removeAll(List.of(3, -0.0))).isFalse();

        assertThat(set).containsExactly(1.0, 2);

        set.clear();

        set.add(1.0);
        set.add(-0.0);
        set.add(2);

        set.removeAll(List.of(0, 2.0));

        assertThat(set).containsExactly(1.0);
    }

    @Test
    void testRetainAll() throws Exception {
        Set<Object> set = new LinkedTreeSet<>();

        set.add(1.0);
        set.add(0);
        set.add(2);

        set.retainAll(List.of(0.0, 2.0));

        assertThat(set).containsExactly(0, 2);
    }

    @Test
    void testContains() throws Exception {
        Set<Object> set1 = new LinkedTreeSet<>();

        assertThat(set1.contains(0)).isFalse();

        set1.add(-0.0);
        set1.add(1);

        assertThat(set1.contains(0)).isTrue();
        assertThat(set1.contains(0.0)).isTrue();
        assertThat(set1.contains(1.0)).isTrue();
        assertThat(set1.contains(1)).isTrue();

        assertThat(set1.contains(2)).isFalse();
    }

    @Test
    void testContainsAll() throws Exception {
        Set<Object> set1 = new LinkedTreeSet<>();
        Set<Object> set2 = new LinkedTreeSet<>();

        assertThat(set1.containsAll(set2)).isTrue();

        set2.add(1);
        set2.add(2);

        assertThat(set1.containsAll(set2)).isFalse();

        set1.add(1.0);
        set1.add(2);
        set1.add(3.0);

        assertThat(set1.containsAll(set2)).isTrue();
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/109
    @Test
    void testSizeAfterRemoveAll() throws Exception {
        Set<Object> set = new LinkedTreeSet<>(List.of("A", "B"));

        set.removeAll(List.of("B", "C", "D"));

        assertThat(set).containsExactly("A");
        assertThat(set).hasSize(1);
    }

    // https://github.com/bwaldvogel/mongo-java-server/issues/109
    @Test
    void testSizeAfterRetainAll() throws Exception {
        Set<Object> set = new LinkedTreeSet<>(List.of("A", "B", "C"));

        set.retainAll(List.of("B", "C"));

        assertThat(set).containsExactly("B", "C");
        assertThat(set).hasSize(2);
    }

}
