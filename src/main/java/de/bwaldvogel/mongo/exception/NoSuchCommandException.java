package de.bwaldvogel.mongo.exception;

import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

public class NoSuchCommandException extends MongoServerException {

    private static final long serialVersionUID = 772416798455878545L;

    private String command;

    public NoSuchCommandException(String command) {
        super( "no such cmd: " + command );
        this.command = command;
    }

    public String getCommand() {
        return command;
    }

    @Override
    public BSONObject createBSONObject( int clientId , BSONObject query ) {
        BasicDBObject obj = new BasicDBObject( "errmsg" , getMessage() );
        obj.put( "bad cmd", query );
        obj.put( "ok", Integer.valueOf( 0 ) );
        return obj;
    }

}
