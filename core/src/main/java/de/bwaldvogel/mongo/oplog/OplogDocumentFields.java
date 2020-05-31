package de.bwaldvogel.mongo.oplog;

import de.bwaldvogel.mongo.backend.Constants;

interface OplogDocumentFields {
    String ID = Constants.ID_FIELD;
    String TIMESTAMP = "ts";
    String O = "o";
    String O2 = "o2";
    String ID_DATA_KEY = "_data";
    String NAMESPACE = "ns";
}
