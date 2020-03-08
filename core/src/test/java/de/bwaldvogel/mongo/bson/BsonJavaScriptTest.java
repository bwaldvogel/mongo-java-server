package de.bwaldvogel.mongo.bson;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BsonJavaScriptTest {

    @Test
    void testEqualsAndHashCode() throws Exception {
        EqualsVerifier.forClass(BsonJavaScript.class).verify();
    }

}
