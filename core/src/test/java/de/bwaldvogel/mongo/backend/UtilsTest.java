package de.bwaldvogel.mongo.backend;

import static org.fest.assertions.Assertions.assertThat;

import java.util.Arrays;
import java.util.Date;

import org.bson.Document;
import org.junit.Test;

public class UtilsTest {

    @Test
    public void testAddNumbers() {
        assertThat(Utils.addNumbers(-4, 3)).isEqualTo(Integer.valueOf(-1));
        assertThat(Utils.addNumbers(-0.1, 0.1)).isEqualTo(Double.valueOf(0.0));
        assertThat(Utils.addNumbers(0.9, 0.1)).isEqualTo(Double.valueOf(1.0));
        assertThat(Utils.addNumbers(4.3f, 7.1f)).isEqualTo(Float.valueOf(11.4f));
        assertThat(Utils.addNumbers((short) 4, (short) 7)).isEqualTo(Short.valueOf((short) 11));
        assertThat(Utils.addNumbers(4l, 7.3)).isEqualTo(Double.valueOf(11.3));
        assertThat(Utils.addNumbers(100000000000000l, 100000000000000l)).isEqualTo(Long.valueOf(200000000000000l));
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
    public void testCalculateSize() {
        assertThat(Utils.calculateSize(new Document())).isEqualTo(5);
        assertThat(Utils.calculateSize(new Document("_id", 7))).isEqualTo(14);
    }

    @Test
    public void testGetSubdocumentValue() throws Exception {
        Document document = new Document("foo", 25);
        assertThat(Utils.getSubdocumentValue(document, "foo")).isEqualTo(25);
        assertThat(Utils.getSubdocumentValue(document, "foo.bar")).isNull();
        assertThat(Utils.getSubdocumentValue(document, "foo.bar.x")).isNull();

        document.put("foo", new Document("a", 10).append("b", new Document("x", 29).append("z", 17)));
        assertThat(Utils.getSubdocumentValue(document, "foo.a")).isEqualTo(10);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.x")).isEqualTo(29);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.z")).isEqualTo(17);
        assertThat(Utils.getSubdocumentValue(document, "foo.c")).isNull();
    }

    @Test
    public void testGetFieldValueListSafe() throws Exception {
        assertThat(Utils.getFieldValueListSafe(null, "foo")).isNull();
        Document document = new Document("foo", 25);
        assertThat(Utils.getFieldValueListSafe(document, "foo")).isEqualTo(25);
        assertThat(Utils.getFieldValueListSafe(Arrays.asList("a", "b", "c"), "1")).isEqualTo("b");
    }

    @Test
    public void testHasFieldValueListSafe() throws Exception {
        assertThat(Utils.hasFieldValueListSafe(null, "foo")).isFalse();
        Document document = new Document("foo", 25);
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
