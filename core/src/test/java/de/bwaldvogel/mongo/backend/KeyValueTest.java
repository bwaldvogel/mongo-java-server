package de.bwaldvogel.mongo.backend;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import nl.jqno.equalsverifier.EqualsVerifier;

public class KeyValueTest {

    @Test
    void testEqualsAndHashCodeContract() throws Exception {
        EqualsVerifier.forClass(KeyValue.class).verify();
    }

    @Test
    void testToString() throws Exception {
        assertThat(new KeyValue(1, 2)).hasToString("{ : 1, : 2 }");
    }

}
