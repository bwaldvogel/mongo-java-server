package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ServerVersionTest {

    @Test
    void testToVersionString() throws Exception {
        assertThat(ServerVersion.MONGO_3_6.toVersionString()).isEqualTo("3.6.0");
        assertThat(ServerVersion.MONGO_4_0.toVersionString()).isEqualTo("4.0.0");
        assertThat(ServerVersion.MONGO_4_2.toVersionString()).isEqualTo("4.2.0");
        assertThat(ServerVersion.MONGO_4_4.toVersionString()).isEqualTo("4.4.0");
        assertThat(ServerVersion.MONGO_5_0.toVersionString()).isEqualTo("5.0.0");
        assertThat(ServerVersion.MONGO_5_1.toVersionString()).isEqualTo("5.1.0");
        assertThat(ServerVersion.MONGO_5_2.toVersionString()).isEqualTo("5.2.0");
        assertThat(ServerVersion.MONGO_5_3.toVersionString()).isEqualTo("5.3.0");
        assertThat(ServerVersion.MONGO_6_0.toVersionString()).isEqualTo("6.0.0");
        assertThat(ServerVersion.MONGO_6_1.toVersionString()).isEqualTo("6.1.0");
        assertThat(ServerVersion.MONGO_6_2.toVersionString()).isEqualTo("6.2.0");
        assertThat(ServerVersion.MONGO_7_0.toVersionString()).isEqualTo("7.0.0");
        assertThat(ServerVersion.MONGO_7_1.toVersionString()).isEqualTo("7.1.0");
        assertThat(ServerVersion.MONGO_7_2.toVersionString()).isEqualTo("7.2.0");
        assertThat(ServerVersion.MONGO_7_3.toVersionString()).isEqualTo("7.3.0");
        assertThat(ServerVersion.MONGO_8_0.toVersionString()).isEqualTo("8.0.0");
        assertThat(ServerVersion.MONGO_9_0.toVersionString()).isEqualTo("9.0.0");
    }

}
