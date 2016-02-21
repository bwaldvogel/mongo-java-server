package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bson.Document;
import org.junit.Test;

public class DocumentComparatorTest {

    @Test
    public void testCompareSingleKey() {
        DocumentComparator comparator = new DocumentComparator(new Document("a", 1));

        List<Document> list = new ArrayList<>();
        list.add(new Document("a", 10));
        list.add(new Document("a", 15));
        list.add(new Document("a", 5));
        list.add(new Document("b", 1));

        Collections.sort(list, comparator);
        assertThat(list).containsExactly(new Document("b", 1), //
                new Document("a", 5), //
                new Document("a", 10), //
                new Document("a", 15));
    }

    @Test
    public void testCompareMultiKey() {
        DocumentComparator comparator = new DocumentComparator(new Document("a", 1).append("b", -1));

        List<Document> list = new ArrayList<>();
        list.add(new Document("a", 15).append("b", 3));
        list.add(new Document("a", 15).append("b", 2));
        list.add(new Document("a", 5));
        list.add(new Document("b", 1));
        list.add(new Document("b", 2));
        list.add(new Document("b", 3));

        Random rnd = new Random(4711);

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(list, rnd);

            Collections.sort(list, comparator);
            assertThat(list).containsExactly(new Document("b", 3), //
                    new Document("b", 2), //
                    new Document("b", 1), //
                    new Document("a", 5), //
                    new Document("a", 15).append("b", 3), //
                    new Document("a", 15).append("b", 2));
        }
    }

    @Test
    public void testCompareCompoundKey() throws Exception {
        DocumentComparator comparator = new DocumentComparator(new Document("a.b", 1).append("c", -1));

        Document a = new Document("a", new Document("b", 10));
        Document b = new Document("a", new Document("b", 15));
        Document c = new Document("a", new Document("b", 15)).append("x", 70);

        assertThat(comparator.compare(a, b)).isLessThan(0);
        assertThat(comparator.compare(b, a)).isGreaterThan(0);
        assertThat(comparator.compare(b, c)).isZero();

    }
}
