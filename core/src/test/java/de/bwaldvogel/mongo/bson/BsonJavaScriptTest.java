package de.bwaldvogel.mongo.bson;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

class BsonJavaScriptTest {

    @Test
    void testEqualsAndHashCode() {
        EqualsVerifier.forClass(BsonJavaScript.class).verify();
    }

}
