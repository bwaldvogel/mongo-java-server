package de.bwaldvogel.mongo.backend;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TestClock extends MongoBackendClock {

    private static final Instant DEFAULT_INSTANT = Instant.parse("2019-05-23T12:00:00.123Z");

    private TestClock(Instant instant, ZoneId zone) {
        super(Clock.fixed(instant, zone));
    }

    public static TestClock defaultClock() {
        return new TestClock(DEFAULT_INSTANT, ZoneOffset.UTC);
    }

    public void reset() {
        super.reset(DEFAULT_INSTANT);
    }

}
