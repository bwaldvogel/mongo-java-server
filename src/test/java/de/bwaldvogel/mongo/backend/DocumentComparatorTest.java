package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;
import org.junit.Test;

public class DocumentComparatorTest {

    @Test
    public void testCompareSingleKey() {
        DocumentComparator comparator = new DocumentComparator(new BasicBSONObject("a", 1));

        List<BSONObject> list = new ArrayList<BSONObject>();
        list.add(new BasicBSONObject("a", 10));
        list.add(new BasicBSONObject("a", 15));
        list.add(new BasicBSONObject("a", 5));
        list.add(new BasicBSONObject("b", 1));

        Collections.sort(list, comparator);
        assertThat(list).containsExactly(new BasicBSONObject("b", 1), //
                new BasicBSONObject("a", 5), //
                new BasicBSONObject("a", 10), //
                new BasicBSONObject("a", 15));
    }

    @Test
    public void testCompareMultiKey() {
        DocumentComparator comparator = new DocumentComparator(new BasicBSONObject("a", 1).append("b", -1));

        List<BSONObject> list = new ArrayList<BSONObject>();
        list.add(new BasicBSONObject("a", 15).append("b", 3));
        list.add(new BasicBSONObject("a", 15).append("b", 2));
        list.add(new BasicBSONObject("a", 5));
        list.add(new BasicBSONObject("b", 1));
        list.add(new BasicBSONObject("b", 2));
        list.add(new BasicBSONObject("b", 3));

        Random rnd = new Random(4711);

        for (int i = 0; i < 10; i++) {
            Collections.shuffle(list, rnd);

            Collections.sort(list, comparator);
            assertThat(list).containsExactly(new BasicBSONObject("b", 3), //
                    new BasicBSONObject("b", 2), //
                    new BasicBSONObject("b", 1), //
                    new BasicBSONObject("a", 5), //
                    new BasicBSONObject("a", 15).append("b", 3), //
                    new BasicBSONObject("a", 15).append("b", 2));
        }
    }

}
