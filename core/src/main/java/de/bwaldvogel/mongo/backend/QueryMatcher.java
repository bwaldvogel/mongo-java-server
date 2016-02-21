package de.bwaldvogel.mongo.backend;

import org.bson.Document;

import de.bwaldvogel.mongo.exception.MongoServerException;

public interface QueryMatcher {

    boolean matches(Document document, Document query) throws MongoServerException;

    boolean matchesValue(Object queryValue, Object value) throws MongoServerException;

    Integer matchPosition(Document document, Document query) throws MongoServerException;

}
