package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Date;

import org.junit.Test;

import de.bwaldvogel.mongo.bson.Document;

public class UtilsTest {

    @Test
    public void testAddNumbers() {
        assertThat(Utils.addNumbers(-4, 3)).isEqualTo(Integer.valueOf(-1));
        assertThat(Utils.addNumbers(-0.1, 0.1)).isEqualTo(Double.valueOf(0.0));
        assertThat(Utils.addNumbers(0.9, 0.1)).isEqualTo(Double.valueOf(1.0));
        assertThat(Utils.addNumbers(4.3f, 7.1f)).isEqualTo(Float.valueOf(11.4f));
        assertThat(Utils.addNumbers((short) 4, (short) 7)).isEqualTo(Short.valueOf((short) 11));
        assertThat(Utils.addNumbers(4L, 7.3)).isEqualTo(Double.valueOf(11.3));
        assertThat(Utils.addNumbers(100000000000000L, 100000000000000L)).isEqualTo(200000000000000L);

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> Utils.addNumbers(new BigDecimal(1), new BigDecimal(1)))
            .withMessage("cannot add 1 and 1");
    }

    @Test
    public void testSubtractNumbers() {
        assertThat(Utils.subtractNumbers(-4, 3)).isEqualTo(Integer.valueOf(-7));
        assertThat(Utils.subtractNumbers(0.1, 0.1)).isEqualTo(Double.valueOf(0.0));
        assertThat(Utils.subtractNumbers(1.1, 0.1)).isEqualTo(Double.valueOf(1.0));
        assertThat(Utils.subtractNumbers(7.6f, 4.1f)).isEqualTo(Float.valueOf(3.5f));
        assertThat(Utils.subtractNumbers((short) 4, (short) 7)).isEqualTo(Short.valueOf((short) -3));
        assertThat(Utils.subtractNumbers(4L, 7.3)).isEqualTo(Double.valueOf(-3.3));
        assertThat(Utils.subtractNumbers(100000000000000L, 1L)).isEqualTo(99999999999999L);

        assertThatExceptionOfType(UnsupportedOperationException.class)
            .isThrownBy(() -> Utils.subtractNumbers(new BigDecimal(1), new BigDecimal(1)))
            .withMessage("cannot subtract 1 and 1");
    }

    @Test
    public void testMarkOkay() throws Exception {
        Document obj = new Document();
        Utils.markOkay(obj);
        assertThat(obj.get("ok")).isEqualTo(Integer.valueOf(1));
    }

    @Test
    public void testIsTrue() throws Exception {
        assertThat(Utils.isTrue("foo")).isTrue();
        assertThat(Utils.isTrue(null)).isFalse();
        assertThat(Utils.isTrue(true)).isTrue();
        assertThat(Utils.isTrue(1)).isTrue();
        assertThat(Utils.isTrue(1.0)).isTrue();
        assertThat(Utils.isTrue(0)).isFalse();
    }

    @Test
    public void testNormalizeValue() {
        assertThat(Utils.normalizeValue(Integer.valueOf(4))).isEqualTo(4.0);
        assertThat(Utils.normalizeValue(null)).isNull();
        assertThat(Utils.normalizeValue(new Date())).isInstanceOf(Date.class);
    }

    @Test
    public void testNullAwareEquals() {
        assertThat(Utils.nullAwareEquals(null, null)).isTrue();
        assertThat(Utils.nullAwareEquals(null, 4)).isFalse();
        assertThat(Utils.nullAwareEquals(4, null)).isFalse();
        assertThat(Utils.nullAwareEquals(4, 4)).isTrue();
        assertThat(Utils.nullAwareEquals(4, 4.0)).isTrue();
        assertThat(Utils.nullAwareEquals(Float.valueOf(3.0f), Double.valueOf(3.0))).isTrue();
        assertThat(Utils.nullAwareEquals(new byte[] {}, new byte[] {})).isTrue();
        assertThat(Utils.nullAwareEquals(new byte[] {}, new byte[] { 0x01 })).isFalse();
        assertThat(Utils.nullAwareEquals(new byte[] { 0x01 }, new byte[] { 0x01 })).isTrue();
        assertThat(Utils.nullAwareEquals(new byte[] { 0x01 }, new byte[] { 0x01, 0x02 })).isFalse();
        assertThat(Utils.nullAwareEquals(new byte[] { 0x01, 0x02, 0x03 }, new byte[] { 0x01, 0x02 })).isFalse();
        assertThat(Utils.nullAwareEquals(new byte[] { 0x01 }, new int[] { 0x01 })).isFalse();
    }

    @Test
    public void testCalculateSize() throws Exception {
        assertThat(Utils.calculateSize(new Document())).isEqualTo(5);
        assertThat(Utils.calculateSize(new Document("_id", 7))).isEqualTo(14);
    }

    @Test
    public void testGetSubdocumentValue() throws Exception {
        Document document = json("foo: 25");
        assertThat(Utils.getSubdocumentValue(document, "foo")).isEqualTo(25);
        assertThat(Utils.getSubdocumentValue(document, "foo.bar")).isNull();
        assertThat(Utils.getSubdocumentValue(document, "foo.bar.x")).isNull();

        document.put("foo", json("a: 10").append("b", json("x: 29").append("z", 17)));
        assertThat(Utils.getSubdocumentValue(document, "foo.a")).isEqualTo(10);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.x")).isEqualTo(29);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.z")).isEqualTo(17);
        assertThat(Utils.getSubdocumentValue(document, "foo.c")).isNull();
    }

    @Test
    public void testGetFieldValueListSafe() throws Exception {
        assertThat(Utils.getFieldValueListSafe(null, "foo")).isNull();
        Document document = json("foo: 25");
        assertThat(Utils.getFieldValueListSafe(document, "foo")).isEqualTo(25);
        assertThat(Utils.getFieldValueListSafe(Arrays.asList("a", "b", "c"), "1")).isEqualTo("b");
    }

    @Test
    public void testHasFieldValueListSafe() throws Exception {
        assertThat(Utils.hasFieldValueListSafe(null, "foo")).isFalse();
        Document document = json("foo: 25");
        assertThat(Utils.hasFieldValueListSafe(document, "foo")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(document, "bar")).isFalse();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "0")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "1")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "2")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "3")).isFalse();
    }

    @Test
    public void testGetDatabaseNameFromFullName() throws Exception {
        assertThat(Utils.getDatabaseNameFromFullName("foo.bar")).isEqualTo("foo");
        assertThat(Utils.getDatabaseNameFromFullName("foo.bar.bla")).isEqualTo("foo");
    }

    @Test
    public void testGetCollectionNameFromFullName() throws Exception {
        assertThat(Utils.getCollectionNameFromFullName("foo.bar")).isEqualTo("bar");
        assertThat(Utils.getCollectionNameFromFullName("foo.bar.bla")).isEqualTo("bar.bla");
    }

}
