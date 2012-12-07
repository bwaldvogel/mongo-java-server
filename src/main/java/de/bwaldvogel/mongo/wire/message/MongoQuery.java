package de.bwaldvogel.mongo.wire.message;

import org.bson.BSONObject;
import org.jboss.netty.channel.Channel;

public class MongoQuery extends ClientRequest {

    private final BSONObject query;
    private final BSONObject returnFieldSelector;
    private boolean slaveOk;

    public MongoQuery(Channel channel , MessageHeader header , String fullCollectionName , BSONObject query , BSONObject returnFieldSelector) {
        super( channel , header , fullCollectionName );
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

    public void setSlaveOk( boolean slaveOk ) {
        this.slaveOk = slaveOk;
    }

    public boolean isSlaveOk() {
        return slaveOk;
    }

}
