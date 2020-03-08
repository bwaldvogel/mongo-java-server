package de.bwaldvogel.mongo.bson;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class LegacyUUIDTest {

    @Test
    void testEqualsAndHashCode() throws Exception {
        EqualsVerifier.forClass(LegacyUUID.class)
            .withNonnullFields("uuid")
            .verify();
    }

    @Test
    void testCompare() throws Exception {
    }

}
