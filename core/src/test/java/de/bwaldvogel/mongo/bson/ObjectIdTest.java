package de.bwaldvogel.mongo.bson;

import static org.fest.assertions.Assertions.assertThat;

import org.junit.Test;

import de.bwaldvogel.mongo.wire.BsonConstants;

public class ObjectIdTest {

    @Test
    public void testHashCodeEquals() throws Exception {
        ObjectId objectId = new ObjectId();
        ObjectId other = new ObjectId();
        assertThat(objectId).isEqualTo(objectId);
        assertThat(objectId.compareTo(objectId)).isEqualTo(0);
        assertThat(objectId.hashCode()).isEqualTo(objectId.hashCode());

        assertThat(objectId).isNotEqualTo(other);
        assertThat(objectId.compareTo(other)).isNotEqualTo(0);
    }

    @Test
    public void testToString() throws Exception {
        String expectedPattern = "^ObjectId\\[[a-f0-9]{" + 2 * BsonConstants.LENGTH_OBJECTID + "}\\]$";
        assertThat(new ObjectId().toString()).matches(expectedPattern);
    }


}