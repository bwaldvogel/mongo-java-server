package de.bwaldvogel.mongo;

import java.util.Collections;
import java.util.List;

public enum ServerVersion implements MongoVersion {
    MONGO_3_6(List.of(3, 6, 0), 6),
    MONGO_4_0(List.of(4, 0, 0), 7),
    MONGO_5_0(List.of(5, 0, 0), 8);

    private final List<Integer> versionArray;
    private final int wireVersion;

    ServerVersion(List<Integer> versionArray, int wireVersion) {
        this.versionArray = versionArray;
        this.wireVersion = wireVersion;
    }

    @Override
    public List<Integer> getVersionArray() {
        return Collections.unmodifiableList(versionArray);
    }

    @Override
    public int getWireVersion() {
        return wireVersion;
    }
}
