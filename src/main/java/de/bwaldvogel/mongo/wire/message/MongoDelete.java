package de.bwaldvogel.mongo.wire.message;

import org.bson.BSONObject;


public class MongoDelete extends ClientRequest {

    private BSONObject selector;

    public MongoDelete(int clientId , MessageHeader header , String fullCollectionName , BSONObject selector) {
        super( clientId , header , fullCollectionName );
        this.selector = selector;
    }

    public BSONObject getSelector(){
        return selector;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( getClass().getSimpleName() ).append( "(" );
        sb.append( "header: " ).append( getHeader() );
        sb.append( ", collection: " ).append( getFullCollectionName() );
        sb.append( ", selector: " ).append( selector );
        sb.append( ")" );
        return sb.toString();
    }

}
