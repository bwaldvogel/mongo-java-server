package de.bwaldvogel.mongo.backend;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.exception.MongoServerException;

public interface QueryMatcher {

    boolean matches(BSONObject document, BSONObject query) throws MongoServerException;

    boolean matchesValue(Object queryValue, Object value) throws MongoServerException;

    Integer matchPosition(BSONObject document, BSONObject query) throws MongoServerException;

}
