package de.bwaldvogel.mongo.oplog;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class NoopOplogTest {

    @Test
    void testSingleton() {
        NoopOplog oplog = NoopOplog.get();
        NoopOplog oplog1 = NoopOplog.get();
        assertThat(oplog).isSameAs(oplog1);
    }
}
