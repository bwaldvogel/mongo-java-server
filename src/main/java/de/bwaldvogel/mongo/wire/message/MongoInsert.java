package de.bwaldvogel.mongo.wire.message;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.bson.BSONObject;


public class MongoInsert extends ClientRequest {

    private final List<BSONObject> documents;

    public MongoInsert(int clientId , MessageHeader header , String fullCollectionName , List<BSONObject> documents) {
        super( clientId , header , fullCollectionName );
        this.documents = new ArrayList<BSONObject>( documents );
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( getClass().getSimpleName() ).append( "(" );
        sb.append( "header: " ).append( getHeader() );
        sb.append( ", collection: " ).append( getFullCollectionName() );
        sb.append( ", #documents: " ).append( documents.size() );
        sb.append( ")" );
        return sb.toString();
    }

    public List<BSONObject> getDocuments(){
        return Collections.unmodifiableList( documents );
    }

}
