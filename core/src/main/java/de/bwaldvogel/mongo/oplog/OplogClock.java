package de.bwaldvogel.mongo.oplog;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import de.bwaldvogel.mongo.bson.BsonTimestamp;

class OplogClock {

    private final Clock clock;
    private final AtomicInteger atomicIncrement = new AtomicInteger();

    OplogClock(Clock clock) {
        this.clock = clock;
    }

    BsonTimestamp incrementAndGet() {
        return new BsonTimestamp(clock.instant(), atomicIncrement.incrementAndGet());
    }

    BsonTimestamp now() {
        return new BsonTimestamp(clock.instant(), atomicIncrement.get());
    }

    Instant instant() {
        return clock.instant();
    }

}
