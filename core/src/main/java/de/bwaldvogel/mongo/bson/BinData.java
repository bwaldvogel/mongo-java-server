package de.bwaldvogel.mongo.bson;

import java.util.Arrays;
import java.util.Objects;

public final class BinData implements Comparable<BinData>, Bson {

    private static final long serialVersionUID = 1L;

    private final byte[] data;

    public BinData(byte[] data) {
        this.data = Objects.requireNonNull(data);
    }

    public byte[] getData() {
        return data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinData binData = (BinData) o;
        return Arrays.equals(data, binData.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public int compareTo(BinData other) {
        byte[] bytes1 = getData();
        byte[] bytes2 = other.getData();
        if (bytes1.length != bytes2.length) {
            return Integer.compare(bytes1.length, bytes2.length);
        } else {
            for (int i = 0; i < bytes1.length; i++) {
                int compare = compareUnsigned(bytes1[i], bytes2[i]);
                if (compare != 0) return compare;
            }
            return 0;
        }
    }

    // lexicographic byte comparison 0x00 < 0xFF
    private static int compareUnsigned(byte b1, byte b2) {
        int v1 = (int) b1 & 0xFF;
        int v2 = (int) b2 & 0xFF;
        return Integer.compare(v1, v2);
    }

}
