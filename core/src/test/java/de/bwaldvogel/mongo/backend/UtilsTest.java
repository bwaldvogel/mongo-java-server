package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;

public class UtilsTest {

    @Test
    void testMarkOkay() throws Exception {
        Document obj = new Document();
        Utils.markOkay(obj);
        assertThat(obj.get("ok")).isEqualTo(Double.valueOf(1.0));
    }

    @Test
    void testIsTrue() throws Exception {
        assertThat(Utils.isTrue("foo")).isTrue();
        assertThat(Utils.isTrue(null)).isFalse();
        assertThat(Utils.isTrue(true)).isTrue();
        assertThat(Utils.isTrue(1)).isTrue();
        assertThat(Utils.isTrue(1.0)).isTrue();
        assertThat(Utils.isTrue(0)).isFalse();
        assertThat(Utils.isTrue(Missing.getInstance())).isFalse();
    }

    @Test
    void testNormalizeValue() {
        assertThat(Utils.normalizeValue(null)).isNull();
        assertThat(Utils.normalizeValue(Integer.valueOf(4))).isEqualTo(4.0);
        assertThat(Utils.normalizeValue(-0.0)).isEqualTo(0.0);
        assertThat(Utils.normalizeValue(0.0)).isEqualTo(0.0);
        assertThat(Utils.normalizeValue(Missing.getInstance())).isNull();
        assertThat(Utils.normalizeValue(Instant.now())).isInstanceOf(Instant.class);
        assertThat(Utils.normalizeValue(json("a: {b: 1}"))).isEqualTo(json("a: {b: 1.0}"));
        assertThat(Utils.normalizeValue(json("a: {b: -0.0, c: -1}"))).isEqualTo(json("a: {b: 0.0, c: -1.0}"));
        assertThat(Utils.normalizeValue(json("a: {c: 1, b: 0}")))
            .isNotEqualTo(json("a: {b: 0.0, c: 1.0}"))
            .isEqualTo(json("a: {c: 1.0, b: 0.0}"))
        ;
    }

    @Test
    void testNormalizeLongValue() {
        assertThat(((Double) Utils.normalizeValue(Long.MAX_VALUE)).longValue()).isEqualTo(Long.MAX_VALUE);
        assertThat(((Double) Utils.normalizeValue(Long.MIN_VALUE)).longValue()).isEqualTo(Long.MIN_VALUE);
        assertThat(Utils.normalizeValue(0x7fffffffffffffL)).isEqualTo(36028797018963967L);
        assertThat(Utils.normalizeValue(0x7ffffffffffffffL)).isEqualTo(576460752303423487L);
        assertThat(Utils.normalizeValue(199999999999999999L)).isEqualTo(199999999999999999L);
        assertThat(Utils.normalizeValue(99999999999999998L)).isEqualTo(99999999999999998L);
        assertThat(Utils.normalizeValue(-99999999999999998L)).isEqualTo(-99999999999999998L);
        assertThat(Utils.normalizeValue(9999999999999999L)).isEqualTo(9999999999999999L);
        assertThat(Utils.normalizeValue(999999999999999L)).isEqualTo(999999999999999.0);
    }

    @Test
    void testNormalizeNumber() throws Exception {
        assertThat(Utils.normalizeNumber(null)).isNull();
        assertThat(Utils.normalizeNumber(0)).isEqualTo(0);
        assertThat(Utils.normalizeNumber(1.0)).isEqualTo(1);
        assertThat(Utils.normalizeNumber(-1.0)).isEqualTo(-1);
        assertThat(Utils.normalizeNumber(10000000000.0)).isEqualTo(10000000000L);
        assertThat(Utils.normalizeNumber(3.5)).isEqualTo(3.5);
        assertThat(Utils.normalizeNumber(3.5F)).isEqualTo(3.5);
    }

    @Test
    void testNullAwareEquals() {
        assertThat(Utils.nullAwareEquals(null, null)).isTrue();
        assertThat(Utils.nullAwareEquals(null, Missing.getInstance())).isTrue();
        assertThat(Utils.nullAwareEquals(Missing.getInstance(), null)).isTrue();
        assertThat(Utils.nullAwareEquals(Missing.getInstance(), Missing.getInstance())).isTrue();
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
        assertThat(Utils.nullAwareEquals(json("a: 1"), json("a: 1"))).isTrue();
        assertThat(Utils.nullAwareEquals(json("a: 1"), json("a: 1.0"))).isTrue();
        assertThat(Utils.nullAwareEquals(json("a: 0"), json("a: -0.0"))).isTrue();
        assertThat(Utils.nullAwareEquals(json("a: 0, b: 1"), json("a: 0, b: 1"))).isTrue();
        assertThat(Utils.nullAwareEquals(json("a: 0, b: 1"), json("a: 0, b: 0"))).isFalse();
        assertThat(Utils.nullAwareEquals(json("a: 0, b: 1"), json("b: 1, a: 0"))).isFalse();
    }

    @Test
    void testCalculateSize() throws Exception {
        assertThat(Utils.calculateSize(new Document())).isEqualTo(5);
        assertThat(Utils.calculateSize(new Document("_id", 7))).isEqualTo(14);
    }

    @Test
    void testGetSubdocumentValue() throws Exception {
        Document document = json("foo: 25");
        assertThat(Utils.getSubdocumentValue(document, "foo")).isEqualTo(25);
        assertThat(Utils.getSubdocumentValue(document, "foo.bar")).isInstanceOf(Missing.class);
        assertThat(Utils.getSubdocumentValue(document, "foo.bar.x")).isInstanceOf(Missing.class);

        assertThat(Utils.getSubdocumentValue(json("foo: {bar: null}"), "foo.bar")).isNull();

        document.put("foo", json("a: 10").append("b", json("x: 29").append("z", 17)));
        assertThat(Utils.getSubdocumentValue(document, "foo.a")).isEqualTo(10);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.x")).isEqualTo(29);
        assertThat(Utils.getSubdocumentValue(document, "foo.b.z")).isEqualTo(17);
        assertThat(Utils.getSubdocumentValue(document, "foo.c")).isInstanceOf(Missing.class);

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> Utils.getSubdocumentValue(document, "a."))
            .withMessageContaining("[Error 40353] FieldPath must not end with a '.'.");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> Utils.getSubdocumentValue(document, "a..1"))
            .withMessageContaining("[Error 15998] FieldPath field names may not be empty strings.");

        assertThatExceptionOfType(MongoServerException.class)
            .isThrownBy(() -> Utils.getSubdocumentValue(document, ".a"))
            .withMessageContaining("[Error 15998] FieldPath field names may not be empty strings.");
    }

    @Test
    void testGetFieldValueListSafe() throws Exception {
        assertThat(Utils.getFieldValueListSafe(Missing.getInstance(), "foo")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(null, "foo")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(json(""), "foo")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(json("foo: null"), "foo")).isNull();
        assertThat(Utils.getFieldValueListSafe(json("foo: 25"), "foo")).isEqualTo(25);
        assertThat(Utils.getFieldValueListSafe(Arrays.asList("a", "b", "c"), "1")).isEqualTo("b");
        assertThat(Utils.getFieldValueListSafe(Arrays.asList("a", "b", "c"), "10")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(Collections.emptyList(), "0")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(123, "0")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe("abc", "0")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe(123, "x")).isInstanceOf(Missing.class);
        assertThat(Utils.getFieldValueListSafe("abc", "x")).isInstanceOf(Missing.class);

        List<?> values = Arrays.asList(json("a: 1"), 2, json("a: 2"), json("b: 3"), json("a: {b: 'x'}"));
        assertThat(Utils.getFieldValueListSafe(values, "a"))
            .isEqualTo(Arrays.asList(1, 2, json("b: 'x'")));

        assertThat(Utils.getFieldValueListSafe(Arrays.asList(1, 2, 3), "a")).isInstanceOf(Missing.class);
    }

    @Test
    void testHasFieldValueListSafe() throws Exception {
        assertThat(Utils.hasFieldValueListSafe(null, "foo")).isFalse();
        Document document = json("foo: 25");
        assertThat(Utils.hasFieldValueListSafe(document, "foo")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(document, "bar")).isFalse();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "0")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "1")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "2")).isTrue();
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "3")).isFalse();
        // https://github.com/bwaldvogel/mongo-java-server/issues/61
        assertThat(Utils.hasFieldValueListSafe(Arrays.asList("a", "b", "c"), "foo")).isFalse();
    }

    @Test
    void testGetDatabaseNameFromFullName() throws Exception {
        assertThat(Utils.getDatabaseNameFromFullName("foo.bar")).isEqualTo("foo");
        assertThat(Utils.getDatabaseNameFromFullName("foo.bar.bla")).isEqualTo("foo");
    }

    @Test
    void testGetCollectionNameFromFullName() throws Exception {
        assertThat(Utils.getCollectionNameFromFullName("foo.bar")).isEqualTo("bar");
        assertThat(Utils.getCollectionNameFromFullName("foo.bar.bla")).isEqualTo("bar.bla");
    }

    @Test
    void testChangeSubdocumentValue() throws Exception {
        Document document = json("_id: 1, foo: {bar: 1, bla: 2}");

        Utils.changeSubdocumentValue(document, "foo.bar", 3);
        assertThat(document).isEqualTo(json("_id: 1, foo: {bar: 3, bla: 2}"));

        Utils.changeSubdocumentValue(document, "foo.z", "value");
        assertThat(document).isEqualTo(json("_id: 1, foo: {bar: 3, bla: 2, z: 'value'}"));

        Utils.changeSubdocumentValue(document, "foo", json("x: [1, 2, 3]"));
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, 2, 3]}"));

        Utils.changeSubdocumentValue(document, "foo.x.1", "new-value");
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, 'new-value', 3]}"));

        Utils.changeSubdocumentValue(document, "foo.y", Arrays.asList(1, 2, 3));
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, 'new-value', 3], y: [1, 2, 3]}"));
    }

    @Test
    void testCopySubdocumentValue() throws Exception {
        Document document = json("_id: 1, foo: {bar: 1, bla: 2}, x: null, y: {a: 1, b: [1, 2, 3]}");
        Document result = new Document();

        Utils.copySubdocumentValue(document, result, "_id");
        assertThat(result).isEqualTo(json("_id: 1"));

        Utils.copySubdocumentValue(document, result, "does-not-exist");
        assertThat(result).isEqualTo(json("_id: 1"));

        Utils.copySubdocumentValue(document, result, "foo.bar");
        assertThat(result).isEqualTo(json("_id: 1, foo: {bar: 1}"));

        Utils.copySubdocumentValue(document, result, "foo.bla");
        assertThat(result).isEqualTo(json("_id: 1, foo: {bar: 1, bla: 2}"));

        Utils.copySubdocumentValue(document, result, "x");
        assertThat(result).isEqualTo(json("_id: 1, foo: {bar: 1, bla: 2}, x: null"));

        Utils.copySubdocumentValue(document, result, "y");
        assertThat(result).isEqualTo(json("_id: 1, foo: {bar: 1, bla: 2}, x: null, y: {a: 1, b: [1, 2, 3]}"));
    }

    @Test
    void testRemoveSubdocumentValue() throws Exception {
        Document document = json("_id: 1, foo: {bar: 1, bla: 2}, baz: { bar: { a: 1, b: 2 } }");

        Object removedValue = Utils.removeSubdocumentValue(document, "foo.bar");
        assertThat(removedValue).isEqualTo(1);
        assertThat(document).isEqualTo(json("_id: 1, foo: {bla: 2}, baz: { bar: { a: 1, b: 2 } }"));

        removedValue = Utils.removeSubdocumentValue(document, "foo.bla");
        assertThat(removedValue).isEqualTo(2);
        assertThat(document).isEqualTo(json("_id: 1, foo: {}, baz: { bar: { a: 1, b: 2 } }"));

        removedValue = Utils.removeSubdocumentValue(document, "foo.missing.a");
        assertThat(removedValue).isEqualTo(Missing.getInstance());
        assertThat(document).isEqualTo(json("_id: 1, foo: {}, baz: { bar: { a: 1, b: 2 } }"));

        Utils.changeSubdocumentValue(document, "foo", json("x: [1, 2, 3]"));
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, 2, 3]}, baz: { bar: { a: 1, b: 2 } }"));

        Utils.removeSubdocumentValue(document, "foo.x.1");
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, null, 3]}, baz: { bar: { a: 1, b: 2 } }"));

        Utils.removeSubdocumentValue(document, "foo.x.a");
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, null, 3]}, baz: { bar: { a: 1, b: 2 } }"));

        Utils.removeSubdocumentValue(document, "baz.bar.a.z");
        assertThat(document).isEqualTo(json("_id: 1, foo: {x: [1, null, 3]}, baz: { bar: { a: 1, b: 2 } }"));
    }

    @Test
    void testRemoveSubdocumentValue_array() throws Exception {
        Document document = json("_id: 1, x: [{a: 1, b: 2, c: 3}, {b: 3}]");

        Object removedValues = Utils.removeSubdocumentValue(document, "x.b");
        assertThat(removedValues).isEqualTo(Arrays.asList(2, 3));
        assertThat(document).isEqualTo(json("_id: 1, x: [{a: 1, c: 3}, {}]"));
    }

    @Test
    void testRemoveSubdocumentValue_arrayOnLevel1() throws Exception {
        Document document = json("a: [{b: [{c: 1}, {c: 2, d: 3}, 'abc']}]");

        Object removedValues = Utils.removeSubdocumentValue(document, "a.b.c");
        assertThat(removedValues).isEqualTo(Arrays.asList(1, 2));
        assertThat(document).isEqualTo(json("a: [{b: [{}, {d: 3}, 'abc']}]"));
    }

    @Test
    void testRemoveSubdocumentValue_arrayOnLevel2() throws Exception {
        Document document = json("a: {b: [{c: 1}, {c: 2, d: 3}, 'abc']}");

        Object removedValues = Utils.removeSubdocumentValue(document, "a.b.c");
        assertThat(removedValues).isEqualTo(Arrays.asList(1, 2));
        assertThat(document).isEqualTo(json("a: {b: [{}, {d: 3}, 'abc']}"));
    }

    @Test
    void testRemoveSubdocumentValue_arrayValue() throws Exception {
        Document document = json("a: {b: {c: [1, 2, 3]}}");

        Object removedValues = Utils.removeSubdocumentValue(document, "a.b.c");
        assertThat(removedValues).isEqualTo(Arrays.asList(1, 2, 3));
        assertThat(document).isEqualTo(json("a: {b: {}}"));
    }

    @Test
    void testCanFullyTraverseSubkeyForRename() {
        Document document = json("_id: 1, foo: {bar: 1, bla: 2}, baz: { bar: [ { a:1, b:2} , 2, 3] }");

        boolean ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "foo.bar");
        assertThat(ableToTraverse).isTrue();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "foo.bar.missing");
        assertThat(ableToTraverse).isFalse();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "foo.missing");
        assertThat(ableToTraverse).isTrue();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "baz.bar");
        assertThat(ableToTraverse).isTrue();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "baz.bar.0");
        assertThat(ableToTraverse).isFalse();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "baz.bar.0.a");
        assertThat(ableToTraverse).isFalse();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "baz.bar.foo");
        assertThat(ableToTraverse).isFalse();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "missing");
        assertThat(ableToTraverse).isTrue();

        ableToTraverse = Utils.canFullyTraverseSubkeyForRename(document, "missing.a");
        assertThat(ableToTraverse).isTrue();
    }

    @Test
    void testShorterPathIfPrefix() throws Exception {
        assertThat(Utils.getShorterPathIfPrefix("a", "b")).isNull();
        assertThat(Utils.getShorterPathIfPrefix("a.b", "b.c")).isNull();
        assertThat(Utils.getShorterPathIfPrefix("a.b.c", "a.b.d")).isNull();
        assertThat(Utils.getShorterPathIfPrefix("deleted", "deletedBy")).isNull();
        assertThat(Utils.getShorterPathIfPrefix("a.deleted", "a.deletedBy")).isNull();
        assertThat(Utils.getShorterPathIfPrefix("a", "a")).isEqualTo("a");
        assertThat(Utils.getShorterPathIfPrefix("a.b", "a.b")).isEqualTo("a.b");
        assertThat(Utils.getShorterPathIfPrefix("a.b.c", "a")).isEqualTo("a");
        assertThat(Utils.getShorterPathIfPrefix("a.b.c", "a.b")).isEqualTo("a.b");
        assertThat(Utils.getShorterPathIfPrefix("a.b.c", "a.b.c")).isEqualTo("a.b.c");
    }

    @Test
    void testJoinTail() throws Exception {
        assertThat(Utils.joinTail(Collections.singletonList("a"))).isEqualTo("");
        assertThat(Utils.joinTail(Collections.emptyList())).isEqualTo("");
        assertThat(Utils.joinTail(Arrays.asList("a", "b"))).isEqualTo("b");
        assertThat(Utils.joinTail(Arrays.asList("a", "b", "c"))).isEqualTo("b.c");
        assertThat(Utils.joinTail(Arrays.asList("a", "b", "c", "d"))).isEqualTo("b.c.d");
    }

    @Test
    void testCollectCommonPathFragments() throws Exception {
        assertThat(Utils.collectCommonPathFragments("a", "b")).isEmpty();
        assertThat(Utils.collectCommonPathFragments("aaa", "a.aab")).isEmpty();
        assertThat(Utils.collectCommonPathFragments("a.a", "a.b")).containsExactly("a");
        assertThat(Utils.collectCommonPathFragments("a.a", "a.a.b")).containsExactly("a", "a");
        assertThat(Utils.collectCommonPathFragments("foo.bar", "foo.bar.bla")).containsExactly("foo", "bar");
        assertThat(Utils.collectCommonPathFragments("a.a", "a.b.c")).containsExactly("a");
        assertThat(Utils.collectCommonPathFragments("ab.c", "a.b.c")).isEmpty();
    }

}
