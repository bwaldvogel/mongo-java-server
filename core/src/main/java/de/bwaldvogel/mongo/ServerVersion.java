package de.bwaldvogel.mongo;

import java.util.Collections;
import java.util.List;

import de.bwaldvogel.mongo.backend.Utils;

public enum ServerVersion implements MongoVersion {
    MONGO_3_6(List.of(3, 6, 0), 6);

    private final List<Integer> versionArray;
    private final int wireVersion;

    ServerVersion(List<Integer> versionArray, int wireVersion) {
        this.versionArray = versionArray;
        this.wireVersion = wireVersion;
    }

    public List<Integer> getVersionArray() {
        return Collections.unmodifiableList(versionArray);
    }

    public String toVersionString() {
        return Utils.join(versionArray, ".");
    }

    public int getWireVersion() {
        return wireVersion;
    }
}
