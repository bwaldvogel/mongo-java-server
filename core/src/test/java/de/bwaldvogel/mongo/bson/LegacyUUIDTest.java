package de.bwaldvogel.mongo.bson;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

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
        UUID uuid0 = UUID.fromString("00000000-0000-0000-0000-000000000000");
        UUID uuid1 = UUID.fromString("48fe9251-9502-4d43-bd81-b8861eb69dc5");
        UUID uuid2 = UUID.fromString("cb6bdaa5-d134-4244-9383-ccded62a7bb3");
        UUID uuid3 = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

        assertComparesLessThan(uuid0, uuid1);
        assertComparesLessThan(uuid2, uuid0);
        assertComparesLessThan(uuid2, uuid1);
        assertComparesLessThan(uuid2, uuid3);
        assertComparesLessThan(uuid3, uuid0);
    }

    private static void assertComparesLessThan(UUID a, UUID b) {
        assertComparesEqual(a);
        assertComparesEqual(b);
        assertThat(new LegacyUUID(a).compareTo(new LegacyUUID(b))).as(a + " < " + b).isEqualTo(-1);
        assertThat(new LegacyUUID(b).compareTo(new LegacyUUID(a))).as(b + " > " + a).isEqualTo(1);
    }

    private static void assertComparesEqual(UUID uuid) {
        assertThat(new LegacyUUID(uuid).compareTo(new LegacyUUID(uuid))).isEqualTo(0);
    }

}
