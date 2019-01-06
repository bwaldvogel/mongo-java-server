package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static de.bwaldvogel.mongo.wire.BsonConstants.LENGTH_OBJECTID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.ObjectId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ValueComparatorTest {

    private ValueComparator comparator = new ValueComparator();

    @Test
    public void testCompareNullsAndMissings() {
        assertThat(comparator.compare(null, null)).isZero();
        assertThat(comparator.compare(Missing.getInstance(), Missing.getInstance())).isZero();
        assertThat(comparator.compare(null, Missing.getInstance())).isZero();
        assertThat(comparator.compare(Missing.getInstance(), null)).isZero();
    }

    @Test
    public void testCompareNullWithValue() throws Exception {
        assertFirstValueBeforeSecondValue(null, 1.0);
    }

    @Test
    public void testCompareList() throws Exception {
        assertFirstValueBeforeSecondValue(1, Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue("abc", Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue(json("a: 1"), Arrays.asList(1, 2));
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), true);
        assertFirstValueBeforeSecondValue(Arrays.asList(1, 2), new ObjectId());
    }

    @Test
    public void testCompareMissingWithValue() throws Exception {
        assertFirstValueBeforeSecondValue(Missing.getInstance(), 1.0);
    }

    @Test
    public void testCompareObjectIds() {
        assertThat(comparator.compare(objectId(123000), objectId(123000))).isZero();
        assertFirstValueBeforeSecondValue(objectId(123000), objectId(223000));
        assertFirstValueBeforeSecondValue(objectId(123000), objectId(124000));
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

    @Test
    public void testCompareStringValues() {
        assertThat(comparator.compare("abc", "abc")).isZero();
        assertFirstValueBeforeSecondValue("abc", "zzz");
        assertFirstValueBeforeSecondValue(null, "abc");
    }

    @Test
    public void testCompareNumberValues() {
        assertThat(comparator.compare(123, 123.0)).isZero();
        assertFirstValueBeforeSecondValue(17L, 17.3);
        assertFirstValueBeforeSecondValue(58.9999, 59);
        assertFirstValueBeforeSecondValue(null, 27);
        assertThat(comparator.compare(-0.0, 0.0)).isZero();
    }

    @Test
    public void testCompareDateValues() {
        assertThat(comparator.compare(new Date(17), new Date(17))).isZero();
        assertFirstValueBeforeSecondValue(new Date(28), new Date(29));
        assertFirstValueBeforeSecondValue(null, new Date());
    }

    @Test
    public void testCompareByteArrayValues() {
        assertThat(comparator.compare(new byte[] { 1 }, new byte[] { 1 })).isZero();
        assertFirstValueBeforeSecondValue(new byte[] { 1 }, new byte[] { 1, 2 });
        assertFirstValueBeforeSecondValue(new byte[] { 0x00 }, new byte[] { (byte) 0xFF });
        assertFirstValueBeforeSecondValue(null, new byte[] { 1 });
    }

    @Test
    public void testCompareDocuments() throws Exception {
        assertThat(comparator.compare(json("a: 1"), json("a: 1.0"))).isZero();
        assertThat(comparator.compare(json("a: 0"), json("a: -0.0"))).isZero();
        assertThat(comparator.compare(json("a: {b: 1}"), json("a: {b: 1.0}"))).isZero();
        assertDocumentComparison("a: -1", "a: 0");
        assertDocumentComparison("a: 1", "a: 1, b: 1");
        assertDocumentComparison("a: {b: 1}", "a: {c: 2}");
        assertDocumentComparison("a: {b: 1}", "a: {c: 0}");
        assertDocumentComparison("a: {b: -1.0}","a: {b: 1}");
        assertDocumentComparison("a: {b: 1}", "a: {b: 1, c: 1}");
        assertDocumentComparison("a: {b: 1}", "a: {b: 1, c: null}");
        assertDocumentComparison("a: {b: 1, c: 0}", "a: {b: {c: 1}}");
        assertDocumentComparison("a: {b: null, c: 0}", "a: {b: {c: 0}}");
        assertDocumentComparison("a: {c: 0}", "a: {b: 'abc'}");
        assertDocumentComparison("a: {c: 0}", "a: {b: {c: 0}}");
    }

    private void assertDocumentComparison(String document1, String document2) {
        assertFirstValueBeforeSecondValue(json(document1), json(document2));
    }

    private void assertFirstValueBeforeSecondValue(Object value1, Object value2) {
        assertThat(comparator.compare(value1, value2)).isLessThan(0);
        assertThat(comparator.compare(value2, value1)).isGreaterThan(0);
    }

}
