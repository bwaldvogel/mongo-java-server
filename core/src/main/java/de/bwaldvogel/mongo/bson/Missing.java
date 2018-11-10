package de.bwaldvogel.mongo.bson;

public class Missing implements Bson {

    private static final long serialVersionUID = 1L;

    private static final Missing INSTANCE = new Missing();

    private Missing() {
    }

    public static Missing getInstance() {
        return INSTANCE;
    }

}
