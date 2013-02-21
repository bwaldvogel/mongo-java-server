package de.bwaldvogel.mongo.exception;


public class ModFieldNotAllowedError extends MongoServerError {

    private static final long serialVersionUID = 2696896807939190889L;

    public ModFieldNotAllowedError(String field) {
        super(10148, "Mod on " + field + " not allowed");
    }

}
