package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static de.bwaldvogel.mongo.wire.BsonConstants.LENGTH_OBJECTID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.ObjectId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ValueComparatorTest {

    private Comparator<Object> comparator = ValueComparator.asc();

    @Test
    public void testReverse() throws Exception {
        assertThat(ValueComparator.asc().reversed()).isSameAs(ValueComparator.desc());
        assertThat(ValueComparator.desc().reversed()).isSameAs(ValueComparator.asc());
    }

    @Test
    public void testCompareNullsAndMissings() {
        assertComparesTheSame(null, null);
        assertComparesTheSame(Missing.getInstance(), Missing.getInstance());
        assertComparesTheSame(null, Missing.getInstance());
    }

    @Test
    public void testCompareNullWithValue() throws Exception {
        assertFirstValueBeforeSecondValue(null, 1.0);
    }

    @Test
    public void testCompareList() throws Exception {
        assertThat(ValueComparator.ascWithoutListHandling().compare(Arrays.asList(1, 2), Arrays.asList(1))).isGreaterThan(0);
        assertThat(ValueComparator.ascWithoutListHandling().compare(Arrays.asList(1, 2), Arrays.asList(1, 2))).isZero();
        assertThat(ValueComparator.ascWithoutListHandling().compare(Arrays.asList(1, 2.0), Arrays.asList(1.0, 2))).isZero();
    }

    @Test
    public void testCompareListsInAscendingOrder() throws Exception {
        assertComparesTheSame(1, Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), "abc");
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), json("a: 1"));
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), true);
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), new ObjectId());
        assertComparesTheSame(Collections.emptyList(), Collections.emptyList());
        assertComparesTheSame(Collections.singletonList(1), 1);
        assertComparesTheSame(null, Arrays.asList(null, 1, 2));
        assertFirstValueBeforeSecondValue(Collections.singletonList(1), 2);
        assertFirstValueBeforeSecondValue(1, Collections.singletonList(2));
        assertFirstValueBeforeSecondValue(Collections.emptyList(), null);
        assertFirstValueBeforeSecondValue(Collections.emptyList(), Arrays.asList(null, 1, 2, 3));
        assertFirstValueBeforeSecondValue(Collections.emptyList(), Missing.getInstance());

        assertComparesTheSame(Arrays.asList(1, 2, 3), Arrays.asList(1, 2, 3));
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), Arrays.asList(2, 3));
        assertFirstValueBeforeSecondValue(Collections.emptyList(), Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue(Missing.getInstance(), Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue(Collections.emptyList(), Missing.getInstance());
    }

    @Test
    public void testCompareListsInDescendingOrder() throws Exception {
        assertThat(ValueComparator.desc().compare(Arrays.asList(2, 3), Arrays.asList(1, 2))).isLessThan(0);

        assertThat(ValueComparator.desc().compare(Arrays.asList(1, "abc", 2), Collections.singletonList(3))).isLessThan(0);
        assertThat(ValueComparator.desc().compare(Collections.singletonList(3), Arrays.asList(1, "abc", 2))).isGreaterThan(0);
    }

    @Test
    public void testCompareMissingWithValue() throws Exception {
        assertFirstValueBeforeSecondValue(Missing.getInstance(), 1.0);
    }

    @Test
    public void testCompareObjectIds() {
        assertComparesTheSame(objectId(123000), objectId(123000));
        assertFirstValueBeforeSecondValue(objectId(123000), objectId(223000));
        assertFirstValueBeforeSecondValue(objectId(123000), objectId(124000));
    }

    @Test
    public void testCompareStringValues() {
        assertComparesTheSame("abc", "abc");
        assertFirstValueBeforeSecondValue("abc", "zzz");
        assertFirstValueBeforeSecondValue(null, "abc");
    }

    @Test
    public void testCompareNumberValues() {
        assertComparesTheSame(123, 123.0);
        assertFirstValueBeforeSecondValue(17L, 17.3);
        assertFirstValueBeforeSecondValue(58.9999, 59);
        assertFirstValueBeforeSecondValue(null, 27);
        assertComparesTheSame(-0.0, 0.0);
    }

    @Test
    public void testCompareDateValues() {
        assertComparesTheSame(new Date(17), new Date(17));
        assertFirstValueBeforeSecondValue(new Date(28), new Date(29));
        assertFirstValueBeforeSecondValue(null, new Date());
    }

    @Test
    public void testCompareByteArrayValues() {
        assertComparesTheSame(new byte[] { 1 }, new byte[] { 1 });
        assertFirstValueBeforeSecondValue(new byte[] { 1 }, new byte[] { 1, 2 });
        assertFirstValueBeforeSecondValue(new byte[] { 0x00 }, new byte[] { (byte) 0xFF });
        assertFirstValueBeforeSecondValue(null, new byte[] { 1 });
    }

    @Test
    public void testCompareDocuments() throws Exception {
        assertComparesTheSame(json("a: 1"), json("a: 1.0"));
        assertComparesTheSame(json("a: 0"), json("a: -0.0"));
        assertComparesTheSame(json("a: {b: 1}"), json("a: {b: 1.0}"));
        assertDocumentComparison("a: -1", "a: 0");
        assertDocumentComparison("a: 1", "a: 1, b: 1");
        assertDocumentComparison("a: {b: 1}", "a: {c: 2}");
        assertDocumentComparison("a: {b: 1}", "a: {c: 0}");
        assertDocumentComparison("a: {b: -1.0}", "a: {b: 1}");
        assertDocumentComparison("a: {b: 1}", "a: {b: 1, c: 1}");
        assertDocumentComparison("a: {b: 1}", "a: {b: 1, c: null}");
        assertDocumentComparison("a: {b: 1, c: 0}", "a: {b: {c: 1}}");
        assertDocumentComparison("a: {b: null, c: 0}", "a: {b: {c: 0}}");
        assertDocumentComparison("a: {c: 0}", "a: {b: 'abc'}");
        assertDocumentComparison("a: {c: 0}", "a: {b: {c: 0}}");
    }

    @Test
    public void testCompareUuids() throws Exception {
        assertComparesTheSame(new UUID(1, 1), new UUID(1, 1));
        assertFirstValueBeforeSecondValue(null, new UUID(1, 2));
        assertFirstValueBeforeSecondValue(new UUID(0, 1), new UUID(1, 1));
        assertFirstValueBeforeSecondValue(new byte[0], new UUID(0, 1));

        byte[] highBytes = new byte[16];
        for (int i = 0; i < highBytes.length; i++) {
            highBytes[i] = (byte) 0xFF;
        }

        assertFirstValueBeforeSecondValue(new byte[0], highBytes);
        assertFirstValueBeforeSecondValue(highBytes, new UUID(0, 1));
    }

    private void assertDocumentComparison(String document1, String document2) {
        assertFirstValueBeforeSecondValue(json(document1), json(document2));
    }

    private void assertFirstValueBeforeSecondValue(Object value1, Object value2) {
        assertThat(comparator.compare(value1, value2)).isLessThan(0);
        assertThat(comparator.compare(value2, value1)).isGreaterThan(0);
    }

    private void assertComparesTheSame(Object value1, Object value2) {
        assertThat(comparator.compare(value1, value2)).isZero();
        assertThat(comparator.compare(value2, value1)).isZero();
    }

    private ObjectId objectId(long value) {
        return new ObjectId(convert(value));
    }

    private static byte[] convert(long value) {
        ByteBuf buffer = Unpooled.buffer(LENGTH_OBJECTID);
        try {
            buffer.writeLong(value);
            buffer.writeInt(0);
            byte[] data = new byte[LENGTH_OBJECTID];
            System.arraycopy(buffer.array(), 0, data, 0, data.length);
            return data;
        } finally {
            buffer.release();
        }
    }

}
