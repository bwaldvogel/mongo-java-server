package de.bwaldvogel.mongo.oplog;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

class OplogDocumentTest {

    private OplogDocument oplogDocument1;
    private OplogDocument oplogDocument2;

    @BeforeEach
    void setUp() {
        UUID uuid = UUID.randomUUID();

        oplogDocument1 = new OplogDocument()
            .withOperationDocument(new Document())
            .withAdditionalOperationalDocument(new Document())
            .withT(123L).withHash(123L).withT(234L).withUUID(uuid);

        oplogDocument2 = new OplogDocument()
            .withOperationDocument(new Document())
            .withAdditionalOperationalDocument(new Document())
            .withT(123L).withHash(123L).withT(234L).withUUID(uuid);
    }

    @Test
    void testEqualsNull() {
        assertThat(oplogDocument1).isNotEqualTo(null);
    }

    @Test
    void testEqualsSameInstance() {
        assertThat(oplogDocument1).isEqualTo(oplogDocument1);
    }

    @Test
    void testEqualsDifferentType() {
        assertThat(oplogDocument1).isNotEqualTo(new Object());
    }

    @Test
    void testEqualsUsingDocumentEquals() {
        assertThat(oplogDocument1).isEqualTo(oplogDocument2);
    }

    @Test
    void testEqualsHashCodeContract() throws Exception {
        EqualsVerifier.forClass(OplogDocument.class)
            .suppress(Warning.STRICT_INHERITANCE)
            .withNonnullFields("document")
            .verify();
    }

    @Test
    void testHashCode() {
        assertThat(oplogDocument1).hasSameHashCodeAs(oplogDocument1.asDocument());
    }

}
