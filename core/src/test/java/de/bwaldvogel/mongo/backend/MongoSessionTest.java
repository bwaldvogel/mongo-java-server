package de.bwaldvogel.mongo.backend;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MongoSessionTest {

    MongoSession mongoSession;

    @BeforeEach
    public void setup() {
        mongoSession = new MongoSession();
    }

    @Test
    void testHashCode() {
        assertEquals(mongoSession.id.hashCode(), mongoSession.hashCode());
    }

    @Test
    void testEquals() {
        MongoSession eqSession = mongoSession.clone();
        MongoSession diffSession = new MongoSession();

        assertTrue(mongoSession.equals(eqSession));
        assertFalse(mongoSession.equals(diffSession));
    }

    @Test
    void noopSession() {
        MongoSession noopSession = MongoSession.NoopSession();
        assertNull(noopSession.getTransaction());
    }
}