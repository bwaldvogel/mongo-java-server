package de.bwaldvogel.mongo.bson;

import java.io.Serial;

public class MaxKey implements Bson {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final MaxKey INSTANCE = new MaxKey();

    private MaxKey() {
    }

    public static MaxKey getInstance() {
        return INSTANCE;
    }

}
