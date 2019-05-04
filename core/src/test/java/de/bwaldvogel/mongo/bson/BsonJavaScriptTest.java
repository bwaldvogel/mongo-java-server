package de.bwaldvogel.mongo.bson;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class BsonJavaScriptTest {

    @Test
    public void testEqualsAndHashCode() throws Exception {
        EqualsVerifier.forClass(BsonJavaScript.class).verify();
    }

}
