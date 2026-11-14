package de.bwaldvogel.mongo.bson;

import static de.bwaldvogel.mongo.wire.BsonConstants.LENGTH_OBJECTID;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.regex.Pattern;

import de.bwaldvogel.mongo.backend.Assert;

public class ObjectId implements Bson, Comparable<ObjectId> {

    private static final long serialVersionUID = 1L;

    private static final Pattern PATTERN = Pattern.compile("^[a-f0-9]{24}$");

    private final byte[] data = new byte[LENGTH_OBJECTID];

    private static final SecureRandom random = new SecureRandom();

    public ObjectId() {
        random.nextBytes(data);
    }

    public ObjectId(String hexString) {
        if (!PATTERN.matcher(hexString).matches()) {
            throw new IllegalArgumentException("Failed to parse '" + hexString + "'");
        }
        byte[] parsed = HexFormat.of().parseHex(hexString);
        System.arraycopy(parsed, 0, data, 0, data.length);
    }

    public ObjectId(byte[] data) {
        Assert.equals(data.length, LENGTH_OBJECTID, () -> "Length must be " + LENGTH_OBJECTID + " but was " + data.length);
        System.arraycopy(data, 0, this.data, 0, this.data.length);
    }

    public byte[] toByteArray() {
        return data;
    }

    @Override
    public int compareTo(final ObjectId other) {
        return Arrays.compareUnsigned(toByteArray(), other.toByteArray());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ObjectId objectId = (ObjectId) o;

        return Arrays.equals(data, objectId.data);

    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + getHexData() + "]";
    }

    public String getHexData() {
        return HexFormat.of().formatHex(data);
    }

}
