package de.bwaldvogel.mongo;

import java.util.Collections;
import java.util.List;

public enum ServerVersion implements MongoVersion {
    MONGO_3_6(List.of(3, 6, 0), 6),
    MONGO_4_0(List.of(4, 0, 0), 7),
    MONGO_4_2(List.of(4, 2, 0), 8),
    MONGO_4_4(List.of(4, 4, 0), 9),
    MONGO_5_0(List.of(5, 0, 0), 13),
    MONGO_5_1(List.of(5, 1, 0), 14),
    MONGO_5_2(List.of(5, 2, 0), 15),
    MONGO_5_3(List.of(5, 3, 0), 16),
    MONGO_6_0(List.of(6, 0, 0), 17),
    MONGO_6_1(List.of(6, 1, 0), 18),
    MONGO_6_2(List.of(6, 2, 0), 19),
    MONGO_7_0(List.of(7, 0, 0), 21),
    MONGO_7_1(List.of(7, 1, 0), 22),
    MONGO_7_2(List.of(7, 2, 0), 23),
    MONGO_7_3(List.of(7, 3, 0), 24),
    MONGO_8_0(List.of(8, 0, 0), 25),
    MONGO_9_0(List.of(9, 0, 0), 29);

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
