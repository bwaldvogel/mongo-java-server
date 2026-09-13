package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.channel.Channel;
import io.netty.util.concurrent.SingleThreadEventExecutor;

class MongoServerShutdownTest {

    private static final int TIMEOUT_SECONDS = 10;

    @Test
    @Timeout(3 * TIMEOUT_SECONDS)
    void testShutdownNowAfterClientConnectedDuringShutdown() throws Exception {
        CountDownLatch workerHeld = new CountDownLatch(1);
        CountDownLatch secondClientQueued = new CountDownLatch(1);

        MongoBackend backend = mock(MongoBackend.class);
        doAnswer(invocation -> {
            if (workerHeld.getCount() > 0) {
                holdWorker(invocation.getArgument(0), workerHeld, secondClientQueued);
            }
            return null;
        }).when(backend).handleClose(any());

        MongoServer server = new MongoServer(backend);
        try {
            // a single worker thread, so that the second client is registered only after the first client's close
            server.bind(new InetSocketAddress("localhost", 0), 1, 1);
            InetSocketAddress address = server.getLocalAddress();

            new Socket(address.getHostString(), address.getPort()).close();
            assertThat(workerHeld.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            try (Socket secondClient = new Socket(address.getHostString(), address.getPort())) {
                assertThat(secondClientQueued.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

                // the second client is registered after its event loop started to shut down, and stays open
                server.shutdownNow();

                assertTimeoutPreemptively(Duration.ofSeconds(TIMEOUT_SECONDS), server::shutdownNow);
            }
        } finally {
            server.shutdown();
        }
    }

    /**
     * Keeps the only worker thread in the first client's close until the second client's registration is queued
     * behind it and the server has started to shut down.
     */
    private static void holdWorker(Channel channel, CountDownLatch workerHeld, CountDownLatch secondClientQueued) {
        SingleThreadEventExecutor worker = (SingleThreadEventExecutor) channel.eventLoop();
        int pendingTasks = worker.pendingTasks();
        workerHeld.countDown();
        awaitCondition(() -> worker.pendingTasks() > pendingTasks);
        secondClientQueued.countDown();
        awaitCondition(worker::isShuttingDown);
    }

    private static void awaitCondition(BooleanSupplier condition) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Condition not met within " + TIMEOUT_SECONDS + " seconds");
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
    }

}
