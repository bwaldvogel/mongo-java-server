package de.bwaldvogel.mongo;

import java.util.concurrent.atomic.AtomicInteger;

public class RequestIDGenerator implements IDGenerator {

    private final AtomicInteger requestID = new AtomicInteger();

    @Override
    public int next(){
        return requestID.incrementAndGet();
    }

}
