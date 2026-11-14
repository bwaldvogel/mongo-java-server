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
            return Arrays.compareUnsigned(bytes1, bytes2);
        }
    }

}
