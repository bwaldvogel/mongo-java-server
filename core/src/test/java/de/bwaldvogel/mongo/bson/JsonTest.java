package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.backend.Missing;

class JsonTest {

    @Test
    void testToJsonValue() throws Exception {
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
        assertThat(Json.toJsonValue(List.of(1, 2, 3))).isEqualTo("[ 1, 2, 3 ]");
        assertThat(Json.toJsonValue(Collections.emptyList())).isEqualTo("[]");
        assertThat(Json.toJsonValue(Collections.emptySet())).isEqualTo("[]");
        assertThat(Json.toJsonValue(Instant.ofEpochSecond(1234567890L))).isEqualTo("\"2009-02-13T23:31:30Z\"");
        assertThat(Json.toJsonValue(LegacyUUID.fromString("a2963378-b9cb-4255-80bc-e16a3bf156b4"))).isEqualTo("BinData(3, 5542CBB9783396A2B456F13B6AE1BC80)");
        assertThat(Json.toJsonValue(new LegacyUUID(1, 2))).isEqualTo("BinData(3, 01000000000000000200000000000000)");
        assertThat(Json.toJsonValue(new LegacyUUID(999999, 128))).isEqualTo("BinData(3, 3F420F00000000008000000000000000)");
        assertThat(Json.toJsonValue(UUID.fromString("a2963378-b9cb-4255-80bc-e16a3bf156b4"))).isEqualTo("UUID(\"a2963378-b9cb-4255-80bc-e16a3bf156b4\")");
        assertThat(Json.toJsonValue(new Document())).isEqualTo("{}");
        assertThat(Json.toJsonValue(new Document().append("a", 1))).isEqualTo("{\"a\" : 1}");
        assertThat(Json.toJsonValue(Decimal128.ONE)).isEqualTo("1");
        assertThat(Json.toJsonValue(new Decimal128(1, 1))).isEqualTo("1.8446744073709551617E-6157");
        assertThat(Json.toJsonValue(new Decimal128(5, 3476215962376601600L))).isEqualTo("0.5");
        assertThat(Json.toJsonValue(Decimal128.NaN)).isEqualTo("NaN");
        assertThat(Json.toJsonValue(Decimal128.NEGATIVE_ZERO)).isEqualTo("-0");
        assertThat(Json.toJsonValue(Decimal128.POSITIVE_INFINITY)).isEqualTo("Infinity");
        assertThat(Json.toJsonValue(Decimal128.NEGATIVE_INFINITY)).isEqualTo("-Infinity");
        assertThat(Json.toJsonValue(new BinData(new byte[0]))).isEqualTo("BinData(0, )");
        assertThat(Json.toJsonValue(new BinData(new byte[] { 0x20 }))).isEqualTo("BinData(0, 20)");
        assertThat(Json.toJsonValue(new BinData(new byte[] { 0x20, 0x21 }))).isEqualTo("BinData(0, 2021)");
        assertThat(Json.toJsonValue(new BinData(new byte[] { 0x20, 0x21, 0x22 }))).isEqualTo("BinData(0, 202122)");
    }

    @Test
    void testToJsonValueWithCompactKey() throws Exception {
        assertThat(Json.toJsonValue(new Document().append("a", 1), true, "{", "}")).isEqualTo("{a: 1}");
        assertThat(Json.toJsonValue(new Document().append("a", new Document().append("b", 1)), true, "{", "}")).isEqualTo("{a: {b: 1}}");
    }

}
