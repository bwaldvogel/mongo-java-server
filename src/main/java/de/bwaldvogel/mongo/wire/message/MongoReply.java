package de.bwaldvogel.mongo.wire.message;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.BSONObject;


public class MongoReply {
    private final MessageHeader header;
    private final List<? extends BSONObject> documents;
    private long cursorId;
    private int startingFrom;
    private int flags;

    public MongoReply(MessageHeader header , BSONObject document) {
        this( header , Arrays.asList( document ) );
    }

    public MongoReply(MessageHeader header , List<? extends BSONObject> documents) {
        this.header = header;
        this.documents = documents;
    }

    public MessageHeader getHeader(){
        return header;
    }

    public List<BSONObject> getDocuments(){
        return Collections.unmodifiableList( documents );
    }

    public long getCursorId(){
        return cursorId;
    }

    public int getStartingFrom(){
        return startingFrom;
    }

    public int getFlags(){
        return flags;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append( getClass().getSimpleName() );
        sb.append( "(" );
        sb.append( "documents: " ).append( getDocuments() );
        sb.append( ")" );
        return sb.toString();
    }
}
