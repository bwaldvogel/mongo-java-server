package de.bwaldvogel.mongo.backend;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

public class TestClock extends Clock {

    private static final Instant DEFAULT_INSTANT = Instant.parse("2019-05-23T12:00:00.123Z");

    private Instant instant;
    private final ZoneId zone;

    private TestClock(Instant instant, ZoneId zone) {
        this.zone = zone;
        this.instant = instant;
    }

    public static TestClock defaultClock() {
        return new TestClock(DEFAULT_INSTANT, ZoneOffset.UTC);
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return new TestClock(instant, zone);
    }

    @Override
    public Instant instant() {
        return instant;
    }

    public void windForward(Duration duration) {
        instant = instant.plus(duration);
    }

    public void reset() {
        instant = DEFAULT_INSTANT;
    }

}
