package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerError;

public interface QueryMatcher {

    boolean matches(BSONObject document, BSONObject query) throws MongoServerError;

    Integer matchPosition(BSONObject document, BSONObject query) throws MongoServerError;

}
