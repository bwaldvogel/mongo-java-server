package de.bwaldvogel.mongo.wire.message;

import org.bson.BSONObject;

public class MongoQuery extends ClientRequest {

    private final BSONObject query;
    private final BSONObject returnFieldSelector;

    public MongoQuery(int clientId , MessageHeader header , String fullCollectionName , BSONObject query , BSONObject returnFieldSelector) {
        super( clientId , header , fullCollectionName );
        this.query = query;
        this.returnFieldSelector = returnFieldSelector;
    }

    public BSONObject getQuery() {
        return query;
    }

    public BSONObject getReturnFieldSelector() {
        return returnFieldSelector;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append( getClass().getSimpleName() ).append( "(" );
        sb.append( "header: " ).append( getHeader() );
        sb.append( ", collection: " ).append( getFullCollectionName() );
        sb.append( ", query: " ).append( query );
        sb.append( ", returnFieldSelector: " ).append( returnFieldSelector );
        sb.append( ")" );
        return sb.toString();
    }

}
