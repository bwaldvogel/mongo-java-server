package de.bwaldvogel.mongo.bson;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class LegacyUUIDTest {

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualsVerifier.forClass(LegacyUUID.class)
            .withNonnullFields("uuid")
            .verify();
    }

    @Test
    public void testCompare() throws Exception {
    }

}
