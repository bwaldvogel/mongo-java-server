package de.bwaldvogel.mongo.backend;

import java.io.Serializable;

public class NullableKey implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final NullableKey INSTANCE = new NullableKey();

    private NullableKey() {
    }

    public static Object of(Object value) {
        if (value == null) {
            return INSTANCE;
        } else {
            return value;
        }
    }
}