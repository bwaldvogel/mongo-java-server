package de.bwaldvogel.mongo.backend;

import java.util.Iterator;
import java.util.function.Supplier;

public final class CollectionUtils {

    private CollectionUtils() {
    }

    public static <T> T getSingleElement(Iterable<T> iterable) {
        Iterator<T> iterator = iterable.iterator();
        Assert.isTrue(iterator.hasNext(), () -> "Expected one element but got zero");
        T value = iterator.next();
        Assert.isFalse(iterator.hasNext(), () -> "Expected one element but got at least two");
        return value;
    }

    public static <T> T getSingleElement(Iterable<T> iterable, Supplier<? extends RuntimeException> exceptionSupplier) {
        Iterator<T> iterator = iterable.iterator();
        if (!iterator.hasNext()) {
            throw exceptionSupplier.get();
        }
        T value = iterator.next();
        if (iterator.hasNext()) {
            throw exceptionSupplier.get();
        }
        return value;
    }

}
