package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.TestUtils.json;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class DocumentTest {

    @Test
    public void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(Document.class)
            .withNonnullFields("documentAsMap")
            .verify();
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
    }

}