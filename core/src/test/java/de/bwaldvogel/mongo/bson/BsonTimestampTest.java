package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

class BsonTimestampTest {

    @Test
    void testEqualsHashCodeContract() throws Exception {
        EqualsVerifier.forClass(BsonTimestamp.class).verify();
    }

    @Test
    void testToString() throws Exception {
        assertThat(new BsonTimestamp(Instant.ofEpochSecond(123), 456))
            .hasToString("BsonTimestamp[value=528280977864, seconds=123, inc=456]");
    }

}
