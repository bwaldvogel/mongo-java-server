package de.bwaldvogel.mongo;

import java.util.List;

public interface MongoVersion {

    List<Integer> getVersionArray();

    String toVersionString();

    int getWireVersion();

}
