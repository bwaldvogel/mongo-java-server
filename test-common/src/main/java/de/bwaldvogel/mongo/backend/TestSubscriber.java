package de.bwaldvogel.mongo.backend;

import static de.bwaldvogel.mongo.backend.AbstractTest.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestSubscriber<T> implements Subscriber<T> {

    private static final Logger log = LoggerFactory.getLogger(TestSubscriber.class);

    private final List<T> values = Collections.synchronizedList(new ArrayList<>());
    private final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch completeLatch = new CountDownLatch(1);
    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T value) {
        values.add(value);
        subscription.request(1);
    }

    @Override
    public void onError(Throwable t) {
        log.error("Got error", t);
        errors.add(t);
    }

    @Override
    public void onComplete() {
        completeLatch.countDown();
        subscription = null;
    }

    public List<T> values() {
        return new ArrayList<>(values);
    }

    public void awaitTerminalEvent() throws InterruptedException {
        awaitTerminalEvent(10, TimeUnit.SECONDS);
    }

    public void assertNoErrors() {
        assertThat(errors).isEmpty();
    }

    public void awaitCount(int expectedCount) throws InterruptedException {
        assertThat(expectedCount).isPositive();
        while (values.size() != expectedCount) {
            assertThat(values.size()).isLessThan(expectedCount);
            Thread.sleep(10);
        }
    }

    public void awaitTerminalEvent(int value, TimeUnit timeUnit) throws InterruptedException {
        assertThat(value).isPositive();
        assertThat(timeUnit).isNotNull();
        completeLatch.await(value, timeUnit);
    }

    public T getSingleValue() {
        return CollectionUtils.getSingleElement(values);
    }
}
