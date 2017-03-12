package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.wire.BsonConstants.LENGTH_OBJECTID;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;

import de.bwaldvogel.mongo.bson.ObjectId;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ValueComparatorTest {

    private ValueComparator comparator;

    @Before
    public void setUp() {
        comparator = new ValueComparator();
    }

    @Test
    public void testCompareNulls() {
        assertThat(comparator.compare(null, null)).isZero();
    }

    @Test
    public void testCompareObjectIds() {
        assertThat(comparator.compare(objectId(123000), objectId(123000))).isZero();
        assertThat(comparator.compare(objectId(123000), objectId(223000))).isLessThan(0);
        assertThat(comparator.compare(objectId(123000), objectId(124000))).isLessThan(0);
        assertThat(comparator.compare(objectId(323000), objectId(223000))).isGreaterThan(0);
        assertThat(comparator.compare(objectId(125000), objectId(124000))).isGreaterThan(0);
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
        assertThat(comparator.compare("abc", "abc")).isEqualTo(0);
        assertThat(comparator.compare("abc", "zzz")).isLessThan(0);
        assertThat(comparator.compare("zzz", "abc")).isGreaterThan(0);
        assertThat(comparator.compare(null, "abc")).isLessThan(0);
        assertThat(comparator.compare("abc", null)).isGreaterThan(0);
    }

    @Test
    public void testCompareNumberValues() {
        assertThat(comparator.compare(123, 123.0)).isEqualTo(0);
        assertThat(comparator.compare(17l, 17.3)).isLessThan(0);
        assertThat(comparator.compare(59, 58.9999)).isGreaterThan(0);
        assertThat(comparator.compare(null, 27)).isLessThan(0);
        assertThat(comparator.compare(27, null)).isGreaterThan(0);
    }

    @Test
    public void testCompareDateValues() {
        assertThat(comparator.compare(new Date(17), new Date(17))).isEqualTo(0);
        assertThat(comparator.compare(new Date(28), new Date(29))).isLessThan(0);
        assertThat(comparator.compare(null, new Date())).isLessThan(0);
        assertThat(comparator.compare(new Date(), null)).isGreaterThan(0);
    }

    @Test
    public void testCompareByteArrayValues() {
        assertThat(comparator.compare(new byte[]{1}, new byte[]{1})).isEqualTo(0);
        assertThat(comparator.compare(new byte[]{1}, new byte[]{1,2})).isLessThan(0);
        assertThat(comparator.compare(new byte[]{0x00}, new byte[]{(byte)0xFF})).isLessThan(0);
        assertThat(comparator.compare(null, new byte[]{1})).isLessThan(0);
        assertThat(comparator.compare(new byte[]{1}, null)).isGreaterThan(0);
    }

}
