package de.bwaldvogel.mongo.backend;

import de.bwaldvogel.mongo.bson.BsonTimestamp;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicInteger;

public class MongoBackendClock extends Clock {

    private Clock clock;
    private AtomicInteger atomicIncrement = new AtomicInteger();

    public MongoBackendClock() {
        this(Clock.systemDefaultZone());
    }

    public MongoBackendClock(Clock clock) {
        this.clock = clock;
    }

    public synchronized BsonTimestamp increaseAndGet() {
        return new BsonTimestamp(clock.instant().getEpochSecond(), atomicIncrement.incrementAndGet());
    }

    public synchronized BsonTimestamp get() {
        return new BsonTimestamp(clock.instant().getEpochSecond(), atomicIncrement.get());
    }

    public synchronized BsonTimestamp getAndIncrement() {
        return new BsonTimestamp(clock.instant().getEpochSecond(), atomicIncrement.getAndIncrement());
    }

    public synchronized void windForward(Duration duration) {
        clock = Clock.offset(clock, duration);
        atomicIncrement = new AtomicInteger();
    }

    @Override
    public ZoneId getZone() {
        return clock.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return clock.withZone(zone);
    }

    @Override
    public Instant instant() {
        return clock.instant();
    }

    public synchronized void reset(Instant instant) {
        clock = Clock.fixed(instant, Clock.systemDefaultZone().getZone());
        atomicIncrement = new AtomicInteger();
    }
}
