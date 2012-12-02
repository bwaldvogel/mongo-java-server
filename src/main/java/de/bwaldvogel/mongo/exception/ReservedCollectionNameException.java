package de.bwaldvogel.mongo.exception;

public class ReservedCollectionNameException extends MongoServerException {

    private static final long serialVersionUID = 7385539454918648055L;

    public ReservedCollectionNameException(String message) {
        super( 10093 , message );
    }

}
