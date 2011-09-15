package de.bwaldvogel.mongo.database;

import org.bson.BasicBSONObject;

import de.bwaldvogel.mongo.IDGenerator;
import de.bwaldvogel.mongo.MongoQuery;
import de.bwaldvogel.mongo.MongoReply;
import de.bwaldvogel.mongo.collection.MongoCollection;

public class AdminDatabase implements MongoDatabase {

    private final MongoCollection adminCommandCollection;

    @Override
    public String getName(){
        return "admin";
    }

    public AdminDatabase(IDGenerator idGenerator) {
        adminCommandCollection = new AdminCommandCollection( idGenerator );
    }

    @Override
    public MongoCollection resolveCollection( String collectionName ){
        if ( collectionName.equals( "$cmd" ) )
            return adminCommandCollection;
        throw new UnsupportedOperationException();
    }

    private class AdminCommandCollection extends CommonCollection {

        public AdminCommandCollection(IDGenerator idGenerator) {
            super( idGenerator );
        }

        @Override
        public MongoReply handleQuery( MongoQuery query ){
            if ( query.getReturnFieldSelector() != null )
                throw new UnsupportedOperationException();

            return createReply( query, new BasicBSONObject( "ismaster" , Boolean.TRUE ) );
        }

    }

}
