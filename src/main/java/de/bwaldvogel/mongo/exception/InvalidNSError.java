package de.bwaldvogel.mongo.exception;

public class InvalidNSError extends MongoServerError {

    private static final long serialVersionUID = 7385539454918648055L;

    public InvalidNSError(String collection) {
        super( 16256 , "Invalid ns [" + collection + "]" );
    }

}
