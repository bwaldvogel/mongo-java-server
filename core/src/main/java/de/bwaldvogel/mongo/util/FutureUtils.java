package de.bwaldvogel.mongo.util;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class FutureUtils {
    public static <T> CompletableFuture<T> wrap(Supplier<T> supplier) {
        try {
            return CompletableFuture.completedFuture(supplier.get());
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    public static <T> CompletableFuture<T> failedFuture(Throwable cause) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }
}
