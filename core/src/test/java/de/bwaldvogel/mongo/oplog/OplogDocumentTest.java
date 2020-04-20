package de.bwaldvogel.mongo.oplog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDate;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import de.bwaldvogel.mongo.bson.Document;

class OplogDocumentTest {

    OplogDocument oplogDocument;
    OplogDocument oplogDocument1;

    @BeforeEach
    public void setUp() {
        UUID uuid = UUID.randomUUID();
        oplogDocument = new OplogDocument().withOperationDocument(new Document())
            .withAdditionalOperationalDocument(new Document()).withT(123L).withHash(123L).withT(234L).withUUID(uuid);
        oplogDocument1 = new OplogDocument().withOperationDocument(new Document())
            .withAdditionalOperationalDocument(new Document()).withT(123L).withHash(123L).withT(234L).withUUID(uuid);
    }

    @Test
    void testEqualsNull() {
        assertThat(oplogDocument).isNotEqualTo(null);
    }

    @Test
    void testEqualsSameInstance() {
        assertThat(oplogDocument).isEqualTo(oplogDocument);
    }

    @Test
    void testEqualsDifferentType() {
        assertThat(oplogDocument).isNotEqualTo(new Object());
    }

    @Test
    void testEqualsUsingDocumentEquals() {
        assertThat(oplogDocument).isEqualTo(oplogDocument1);
    }

    @Test
    void testHashCode() {
        assertThat(oplogDocument.hashCode()).isEqualTo(oplogDocument.asDocument().hashCode());
    }
}