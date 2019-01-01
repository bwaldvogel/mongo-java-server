package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.UUID;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.Missing;

public class JsonTest {

    @Test
    public void testToJsonValue() throws Exception {
        assertThat(Json.toJsonValue(null)).isEqualTo("null");
        assertThat(Json.toJsonValue(Missing.getInstance())).isEqualTo("null");
        assertThat(Json.toJsonValue(true)).isEqualTo("true");
        assertThat(Json.toJsonValue(Boolean.TRUE)).isEqualTo("true");
        assertThat(Json.toJsonValue("")).isEqualTo("\"\"");
        assertThat(Json.toJsonValue("abc\"\n\t\n\"efg")).isEqualTo("\"abc\\\"\\n\\t\\n\\\"efg\"");
        assertThat(Json.toJsonValue(3.14F)).isEqualTo("3.14");
        assertThat(Json.toJsonValue(1.2345)).isEqualTo("1.2345");
        assertThat(Json.toJsonValue((short) 42)).isEqualTo("42");
        assertThat(Json.toJsonValue(Long.MAX_VALUE)).isEqualTo("9223372036854775807");
        assertThat(Json.toJsonValue(Arrays.asList(1, 2, 3))).isEqualTo("[1, 2, 3]");
        assertThat(Json.toJsonValue(Collections.emptyList())).isEqualTo("[]");
        assertThat(Json.toJsonValue(Collections.emptySet())).isEqualTo("[]");
        assertThat(Json.toJsonValue(new Date(1234567890000L))).isEqualTo("\"2009-02-13T23:31:30Z\"");
        assertThat(Json.toJsonValue(UUID.fromString("a2963378-b9cb-4255-80bc-e16a3bf156b4"))).isEqualTo("\"a2963378-b9cb-4255-80bc-e16a3bf156b4\"");
    }

}