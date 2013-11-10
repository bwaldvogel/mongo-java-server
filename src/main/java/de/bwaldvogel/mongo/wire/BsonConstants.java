package de.bwaldvogel.mongo.wire;

public interface BsonConstants {
    final int MAX_BSON_OBJECT_SIZE = 16 * 1024 * 1024;

    final byte TERMINATING_BYTE = 0x00;
    final byte TYPE_DOUBLE = 0x01;
    final byte TYPE_UTF8_STRING = 0x02;
    final byte TYPE_EMBEDDED_DOCUMENT = 0x03;
    final byte TYPE_ARRAY = 0x04;
    final byte TYPE_DATA = 0x05;
    /**
     * Deprecated
     */
    final byte TYPE_UNDEFINED = 0x06;
    final byte TYPE_OBJECT_ID = 0x07;
    final byte TYPE_BOOLEAN = 0x08;
    final byte TYPE_UTC_DATETIME = 0x09;
    final byte TYPE_NULL = 0x0A;
    final byte TYPE_REGEX = 0x0B;
    /**
     * Deprecated
     */
    final byte TYPE_DBPOINTER = 0x0C;
    final byte TYPE_JAVASCRIPT_CODE = 0x0D;
    /**
     * Deprecated
     */
    final byte TYPE_SYMBOL = 0x0E;
    final byte TYPE_JAVASCRIPT_CODE_WITH_SCOPE = 0x0F;
    final byte TYPE_INT32 = 0x10;
    final byte TYPE_TIMESTAMP = 0x11;
    final byte TYPE_INT64 = 0x12;
    final byte TYPE_MIN_KEY = (byte) 0xFF;
    final byte TYPE_MAX_KEY = 0x7F;

    final byte BOOLEAN_VALUE_FALSE = 0x00;
    final byte BOOLEAN_VALUE_TRUE = 0x01;

    final byte STRING_TERMINATION = 0x00;

    final byte BINARY_SUBTYPE_GENERIC = 0x00;
    final byte BINARY_SUBTYPE_FUNCTION = 0x01;
    final byte BINARY_SUBTYPE_OLD_BINARY = 0x02;
    final byte BINARY_SUBTYPE_OLD_UUID = 0x03;
    final byte BINARY_SUBTYPE_UUID = 0x04;
    final byte BINARY_SUBTYPE_MD5 = 0x05;
    final byte BINARY_SUBTYPE_USER_DEFINED = (byte) 0x80;

    /**
     * Length in bytes
     */
    final int LENGTH_UUID = 128 / 8;

    /**
     * Length in bytes
     */
    final int LENGTH_OBJECTID = 12;

}
