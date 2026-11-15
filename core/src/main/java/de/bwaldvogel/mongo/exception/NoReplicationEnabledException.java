package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class NoReplicationEnabledException extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    public NoReplicationEnabledException() {
        super(76, "NoReplicationEnabled", "not running with --replSet");
        setLogError(false);
    }

}
