package de.bwaldvogel.mongo.database;

import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.IDGenerator;
import de.bwaldvogel.mongo.MongoQuery;
import de.bwaldvogel.mongo.MongoReply;
import de.bwaldvogel.mongo.collection.MongoCollection;
import de.bwaldvogel.mongo.wire.MessageHeader;

public abstract class CommonCollection implements MongoCollection {
    private final IDGenerator idGenerator;

    public CommonCollection(IDGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    protected MongoReply createUnsupportedErrorReply( MongoQuery query ){
        return createReply( query, new BasicBSONObject( "$err" , "unsupported operation" ) );
    }

    protected MongoReply createReply( MongoQuery query , BSONObject message ){
        final MessageHeader header = new MessageHeader( idGenerator.next() , query.getHeader().getRequestID() );
        final MongoReply reply = new MongoReply( header , message );
        return reply;
    }
}
