package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.Missing;
import de.bwaldvogel.mongo.backend.Utils;
import nl.jqno.equalsverifier.EqualsVerifier;

class DocumentTest {

    @Test
    void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(Document.class)
            .withNonnullFields("documentAsMap")
            .verify();
    }

    @Test
    void testEquals() throws Exception {
        assertThat(new Document()).isNotEqualTo(Collections.emptyMap());
        assertThat(new Document()).isNotEqualTo(null);
        assertThat(new Document()).isEqualTo(new Document());
        assertThat(new Document().append("a", 1). append("b", 1)).isEqualTo(new Document().append("a", 1).append("b", 1));
        assertThat(new Document().append("a", 1). append("b", 1)).isNotEqualTo(new Document().append("b", 1).append("a", 1));
    }

    @Test
    void testClone() throws Exception {
        Document original = json("abc: 123");
        Document clone = original.clone();
        assertThat(clone)
            .isNotSameAs(original)
            .isEqualTo(original);
    }

    @Test
    void testConstructor() throws Exception {
        Map<String, Long> data = Collections.singletonMap("key", 123L);
        Document document = new Document(data);
        assertThat(document).containsExactly(entry("key", 123L));
    }

    @Test
    void testCloneDeeply() throws Exception {
        Document original = new Document();
        original.put("subDocument", new Document("_id", 1));
        original.put("sub", new Document("sub", List.of(1, new Document("key", "value"), 3)));
        original.put("null-value", null);
        original.put("set", new LinkedHashSet<>());

        String originalToString = "{\"subDocument\" : {\"_id\" : 1}, \"sub\" : {\"sub\" : [ 1, { \"key\" : \"value\" }, 3 ]}, \"null-value\" : null, \"set\" : []}";
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

        assertThat(deepClone).hasToString("{\"subDocument\" : {\"_id\" : 2}, \"sub\" : {\"sub\" : [ 1, 25, 3 ]}, \"set\" : [ \"more\" ]}");
        assertThat(original).hasToString(originalToString);
    }

    @Test
    void testClear() throws Exception {
        Document document = json("abc: 123");
        document.clear();
        assertThat(document).isEqualTo(json(""));
    }

    @Test
    void testContainsValue() throws Exception {
        Document document = json("abc: 123");
        assertThat(document.containsValue(123)).isTrue();
        assertThat(document.containsValue(42)).isFalse();
    }

    @Test
    void testValues() throws Exception {
        Document document = json("abc: 123, efg: 456");
        assertThat(document.values()).containsExactly(123, 456);
    }

    @Test
    void testToString() throws Exception {
        assertThat(new Document()).hasToString("{}");
        assertThat(new Document("key", "value")).hasToString("{\"key\" : \"value\"}");
        assertThat(new Document("key", new Document("value", 12345L))).hasToString("{\"key\" : {\"value\" : 12345}}");
        assertThat(json("array: [{'123a': {name: 'old'}}]")).hasToString("{\"array\" : [ { \"123a\" : { \"name\" : \"old\" } } ]}");
    }

    @Test
    void testToString_compactKey() throws Exception {
        assertThat(new Document().toString(true))
            .isEqualTo("{}");

        assertThat(new Document("key", "value").toString(true))
            .isEqualTo("{key: \"value\"}");

        assertThat(new Document("key", new Document("value", 12345L)).toString(true))
            .isEqualTo("{key: {value: 12345}}");

        assertThat(json("array: [{'123a': {name: 'old'}}]").toString(true))
            .isEqualTo("{array: [ { 123a: { name: \"old\" } } ]}");
    }

    @Test
    void testGetOrMissing() throws Exception {
        Document document = new Document().append("a", 1);
        assertThat(document.getOrMissing("b")).isInstanceOf(Missing.class);
        assertThat(document.getOrMissing("a")).isEqualTo(1);
    }

}
