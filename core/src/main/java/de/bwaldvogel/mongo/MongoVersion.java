package de.bwaldvogel.mongo;

import java.util.List;

import de.bwaldvogel.mongo.backend.Utils;

public interface MongoVersion {

    List<Integer> getVersionArray();

    default String toVersionString() {
        return Utils.join(getVersionArray(), ".");
    }

    int getWireVersion();

}
