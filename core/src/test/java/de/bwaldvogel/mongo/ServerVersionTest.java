package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ServerVersionTest {

    @Test
    void testToVersionString() throws Exception {
        assertThat(ServerVersion.MONGO_3_6.toVersionString()).isEqualTo("3.6.0");
        assertThat(ServerVersion.MONGO_4_0.toVersionString()).isEqualTo("4.0.0");
    }

}
