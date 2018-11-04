package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;

public class DocumentComparatorTest {

    @Test
    public void testCompareSingleKey() {
        DocumentComparator comparator = new DocumentComparator(json("a: 1"));

        List<Document> list = Arrays.asList(
            json("a: 10"),
            json("a: 15"),
            json("a: 5"),
            json("b: 1"));

        list.sort(comparator);
        assertThat(list).containsExactly(
            json("b: 1"),
            json("a: 5"),
            json("a: 10"),
            json("a: 15"));
    }

    @Test
    public void testCompareMultiKey() {
        DocumentComparator comparator = new DocumentComparator(json("a: 1, b: -1"));

        List<Document> list = Arrays.asList(
            json("a: 15, b: 3"),
            json("a: 15, b: 2"),
            json("a: 5"),
            json("b: 1"),
            json("b: 2"),
            json("b: 3"));

        Random rnd = new Random(4711);

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(list, rnd);

            list.sort(comparator);
            assertThat(list).containsExactly(
                json("b: 3"),
                json("b: 2"),
                json("b: 1"),
                json("a: 5"),
                json("a: 15, b: 3"),
                json("a: 15, b: 2"));
        }
    }

    @Test
    public void testCompareCompoundKey() throws Exception {
        DocumentComparator comparator = new DocumentComparator(json("'a.b': 1, c: -1"));

        Document a = json("a: {b: 10}");
        Document b = json("a: {b: 15}");
        Document c = json("a: {b: 15, x: 70}");

        assertThat(comparator.compare(a, b)).isLessThan(0);
        assertThat(comparator.compare(b, a)).isGreaterThan(0);
        assertThat(comparator.compare(b, c)).isZero();

    }
}
