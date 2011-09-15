package de.bwaldvogel.mongo.collection;

import de.bwaldvogel.mongo.IDGenerator;
import de.bwaldvogel.mongo.MongoQuery;
import de.bwaldvogel.mongo.MongoReply;
import de.bwaldvogel.mongo.database.CommonCollection;
import de.bwaldvogel.mongo.database.MongoDatabase;

public class MemoryCollection extends CommonCollection {

    public MemoryCollection(IDGenerator idGenerator , MongoDatabase database , String collectionName) {
        super( idGenerator );
    }

    @Override
    public MongoReply handleQuery( MongoQuery query ){
        return createUnsupportedErrorReply( query );
    }

}
