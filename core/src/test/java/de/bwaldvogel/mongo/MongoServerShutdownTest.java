package de.bwaldvogel.mongo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.concurrent.SingleThreadEventExecutor;

class MongoServerShutdownTest {

    private static final int TIMEOUT_SECONDS = 10;

    @Test
    @Timeout(3 * TIMEOUT_SECONDS)
    void testShutdownNowAfterClientConnectedDuringShutdown() throws Exception {
        CountDownLatch workerHeld = new CountDownLatch(1);
        CountDownLatch secondClientQueued = new CountDownLatch(1);

        MongoBackend backend = mock(MongoBackend.class);
        MongoServer server = new MongoServer(backend) {
            @Override
            void initChannel(SocketChannel channel) {
                super.initChannel(channel);
                channel.pipeline().addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        super.channelInactive(ctx);
                        if (workerHeld.getCount() > 0) {
                            holdWorker((SingleThreadEventExecutor) ctx.executor(), workerHeld, secondClientQueued);
                        }
                    }
                });
            }
        };
        try {
            // a single worker thread, so that the second client is registered only after the first client's close
            server.bind(new InetSocketAddress("localhost", 0), 1, 1);
            InetSocketAddress address = server.getLocalAddress();

            new Socket(address.getHostString(), address.getPort()).close();
            assertThat(workerHeld.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

            try (Socket secondClient = new Socket(address.getHostString(), address.getPort())) {
                assertThat(secondClient.isConnected()).isTrue();
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
    private static void holdWorker(SingleThreadEventExecutor worker, CountDownLatch workerHeld, CountDownLatch secondClientQueued) {
        int pendingTasks = worker.pendingTasks();
        workerHeld.countDown();
        await().pollInSameThread().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
            .atMost(Duration.ofSeconds(TIMEOUT_SECONDS)).until(() -> worker.pendingTasks() > pendingTasks);
        secondClientQueued.countDown();
        await().pollInSameThread().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(1))
            .atMost(Duration.ofSeconds(TIMEOUT_SECONDS)).until(worker::isShuttingDown);
    }

}
