package de.bwaldvogel.mongo.collection;

import de.bwaldvogel.mongo.MongoQuery;
import de.bwaldvogel.mongo.MongoReply;

public interface MongoCollection {
    public MongoReply handleQuery( MongoQuery query );
}
