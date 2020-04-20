package de.bwaldvogel.mongo.oplog;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NoopOplogTest {

    @Test
    void testSingleton() {
        NoopOplog oplog = NoopOplog.get();
        NoopOplog oplog1 = NoopOplog.get();
        assertEquals(oplog, oplog1);
    }
}