package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

public interface QueryMatcher {

    boolean matches(BSONObject document, BSONObject query);

}
