package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class KeyValueTest {

    @Test
    public void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(KeyValue.class).verify();
    }

    @Test
    public void testToString() throws Exception {
        assertThat(new KeyValue(1, 2)).hasToString("{ : 1, : 2 }");
    }

}
