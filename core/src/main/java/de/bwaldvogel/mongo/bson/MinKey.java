package de.bwaldvogel.mongo.bson;

import java.io.Serial;

public class MinKey implements Bson {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final MinKey INSTANCE = new MinKey();

    private MinKey() {
    }

    public static MinKey getInstance() {
        return INSTANCE;
    }

}
