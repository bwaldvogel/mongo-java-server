package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import de.bwaldvogel.mongo.wire.BsonConstants;

class ObjectIdTest {

    @Test
    void testHashCodeEquals() {
        ObjectId objectId = new ObjectId();
        ObjectId other = new ObjectId();
        assertThat(objectId).isEqualTo(objectId);
        assertThat(objectId.compareTo(objectId)).isEqualTo(0);
        assertThat(objectId.hashCode()).isEqualTo(objectId.hashCode());

        assertThat(objectId).isNotEqualTo(other);
        assertThat(objectId.compareTo(other)).isNotEqualTo(0);
    }

    @Test
    void testToString() {
        String expectedPattern = "^ObjectId\\[[a-f0-9]{" + 2 * BsonConstants.LENGTH_OBJECTID + "}\\]$";
        assertThat(new ObjectId().toString()).matches(expectedPattern);
    }

    @Test
    void testToHexStringAndBack() {
        ObjectId objectId = new ObjectId();
        ObjectId clone = new ObjectId(objectId.getHexData());
        assertThat(clone).isEqualTo(objectId);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "abcdefabcdefabcdefabcdef",
        "abcdef123456789abcdef000",
        "deadbeefcafebabedeadbeef",
    })
    void testFromValidHexString(String hexString) {
        ObjectId objectId = new ObjectId(hexString);
        assertThat(objectId.getHexData()).isEqualTo(hexString);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "",
        " ",
        "abcdefghijklmn",
        "ABCDEFABCDEFABCDEFABCDEF",
        "                        ",
        "abcdefabcdefabcdefabcde",
        "abcdefabcdefabcdefabcdeff"
    })
    void testFromIllegalHexString(String hexString) {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> new ObjectId(hexString))
            .withMessage("Failed to parse '" + hexString + "'");
    }

}
