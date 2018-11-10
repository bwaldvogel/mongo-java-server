package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.UUID;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.Utils;
import nl.jqno.equalsverifier.EqualsVerifier;

public class DocumentTest {

    @Test
    public void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(Document.class)
            .withNonnullFields("documentAsMap")
            .verify();
    }

    @Test
    public void testEquals() throws Exception {
        assertThat(new Document()).isNotEqualTo(Collections.emptyMap());
        assertThat(new Document()).isNotEqualTo(null);
        assertThat(new Document()).isEqualTo(new Document());
    }

    @Test
    public void testClone() throws Exception {
        Document original = json("abc: 123");
        Document clone = original.clone();
        assertThat(clone)
            .isNotSameAs(original)
            .isEqualTo(original);
    }

    @Test
    public void testCloneDeeply() throws Exception {
        Document original = new Document();
        original.put("subDocument", new Document("_id", 1));
        original.put("sub", new Document("sub", Arrays.asList(1, new Document("key", "value"), 3)));
        original.put("null-value", null);
        original.put("set", new LinkedHashSet<>());

        String originalToString = "{\"subDocument\" : {\"_id\" : 1}, \"sub\" : {\"sub\" : [1, {\"key\" : \"value\"}, 3]}, \"null-value\" : null, \"set\" : []}";
        assertThat(original).hasToString(originalToString);

        Document deepClone = original.cloneDeeply();

        assertThat(deepClone)
            .isNotSameAs(original)
            .isEqualTo(original);

        assertThat(deepClone).hasToString(original.toString());

        Utils.changeSubdocumentValue(deepClone, "sub.sub.1", 25);
        Utils.changeSubdocumentValue(deepClone, "subDocument._id", 2);
        deepClone.remove("null-value");
        ((Collection<Object>) deepClone.get("set")).add("more");

        assertThat(deepClone).hasToString("{\"subDocument\" : {\"_id\" : 2}, \"sub\" : {\"sub\" : [1, 25, 3]}, \"set\" : [\"more\"]}");
        assertThat(original).hasToString(originalToString);
    }

    @Test
    public void testClear() throws Exception {
        Document document = json("abc: 123");
        document.clear();
        assertThat(document).isEqualTo(json(""));
    }

    @Test
    public void testContainsValue() throws Exception {
        Document document = json("abc: 123");
        assertThat(document.containsValue(123)).isTrue();
        assertThat(document.containsValue(42)).isFalse();
    }

    @Test
    public void testValues() throws Exception {
        Document document = json("abc: 123, efg: 456");
        assertThat(document.values()).containsExactly(123, 456);
    }

    @Test
    public void testToString() throws Exception {
        assertThat(new Document()).hasToString("{}");
        assertThat(new Document("key", "value")).hasToString("{\"key\" : \"value\"}");
        assertThat(new Document("key", new Document("value", 12345L))).hasToString("{\"key\" : {\"value\" : 12345}}");
        assertThat(json("array: [{'123a': {name: 'old'}}]")).hasToString("{\"array\" : [{\"123a\" : {\"name\" : \"old\"}}]}");
    }

    @Test
    public void testToJsonValue() throws Exception {
        assertThat(Document.toJsonValue(null)).isEqualTo("null");
        assertThat(Document.toJsonValue(true)).isEqualTo("true");
        assertThat(Document.toJsonValue(Boolean.TRUE)).isEqualTo("true");
        assertThat(Document.toJsonValue("")).isEqualTo("\"\"");
        assertThat(Document.toJsonValue("abc\"\n\t\n\"efg")).isEqualTo("\"abc\\\"\\n\\t\\n\\\"efg\"");
        assertThat(Document.toJsonValue(3.14F)).isEqualTo("3.14");
        assertThat(Document.toJsonValue(1.2345)).isEqualTo("1.2345");
        assertThat(Document.toJsonValue((short) 42)).isEqualTo("42");
        assertThat(Document.toJsonValue(Long.MAX_VALUE)).isEqualTo("9223372036854775807");
        assertThat(Document.toJsonValue(Arrays.asList(1, 2, 3))).isEqualTo("[1, 2, 3]");
        assertThat(Document.toJsonValue(Collections.emptyList())).isEqualTo("[]");
        assertThat(Document.toJsonValue(Collections.emptySet())).isEqualTo("[]");
        assertThat(Document.toJsonValue(new Date(1234567890000L))).isEqualTo("\"2009-02-13T23:31:30Z\"");
        assertThat(Document.toJsonValue(UUID.fromString("a2963378-b9cb-4255-80bc-e16a3bf156b4"))).isEqualTo("\"a2963378-b9cb-4255-80bc-e16a3bf156b4\"");
    }

}