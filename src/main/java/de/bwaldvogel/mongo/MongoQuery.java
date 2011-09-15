package de.bwaldvogel.mongo;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.wire.MessageHeader;

public class MongoQuery {

    private final MessageHeader header;
    private final String fullCollectionName;
    private final BSONObject query;
    private final BSONObject returnFieldSelector;

    public MongoQuery(MessageHeader header , String fullCollectionName , BSONObject query , BSONObject returnFieldSelector) {
        this.header = header;
        this.fullCollectionName = fullCollectionName;
        this.query = query;
        this.returnFieldSelector = returnFieldSelector;
    }

    public MessageHeader getHeader(){
        return header;
    }

    public String getFullCollectionName(){
        return fullCollectionName;
    }

    public BSONObject getQuery(){
        return query;
    }

    public BSONObject getReturnFieldSelector(){
        return returnFieldSelector;
    }

}
