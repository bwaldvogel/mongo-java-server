package de.bwaldvogel.mongo.backend;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Constants {

    public static final String ID_FIELD = "_id";

    public static final int MAX_NS_LENGTH = 128;

    public static final String ID_INDEX_NAME = "_id_";

    public static final Set<String> REFERENCE_KEYS = new HashSet<>(Arrays.asList("$ref", "$id"));

}
