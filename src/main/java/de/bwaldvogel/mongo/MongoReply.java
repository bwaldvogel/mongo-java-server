package de.bwaldvogel.mongo;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bson.BSONObject;

import de.bwaldvogel.mongo.wire.MessageHeader;

public class MongoReply {
    private final MessageHeader header;
    private final List<BSONObject> documents;
    private long cursorId;
    private int startingFrom;
    private int flags;

    public MongoReply(MessageHeader header , BSONObject document) {
        this( header , Arrays.asList( document ) );
    }

    public MongoReply(MessageHeader header , List<BSONObject> documents) {
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
}
