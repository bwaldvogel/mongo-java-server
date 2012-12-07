package de.bwaldvogel.mongo.wire;

public interface Flag {
    boolean isSet( int flags );

    int removeFrom( int flags );
}
