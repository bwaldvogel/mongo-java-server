package de.bwaldvogel.mongo.backend;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestSubscriber<T> implements Subscriber<T> {

    private static final Logger log = LoggerFactory.getLogger(TestSubscriber.class);

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private T value;
    private Throwable throwable;
    private Subscription subscription;

    @Override
    public void onSubscribe(Subscription subscription) {
        this.subscription = subscription;
        subscription.request(1);
    }

    @Override
    public void onNext(T value) {
        log.debug("onNext: {}", value);
        Assert.isNull(this.value, () -> "Got a second value: " + this.value + " and " + value);
        this.value = value;
        subscription.cancel();
        countDownLatch.countDown();
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("onError", throwable);
        this.throwable = throwable;
    }

    @Override
    public void onComplete() {
        log.info("onComplete", throwable);
    }

    T awaitSingleValue() throws Exception {
        boolean success = countDownLatch.await(30, TimeUnit.SECONDS);
        Assert.isTrue(success, () -> "Failed waiting countdown latch");
        if (throwable != null) {
            throw new RuntimeException(throwable);
        }
        Assert.notNull(value, () -> "Got no value yet");
        return value;
    }

}
