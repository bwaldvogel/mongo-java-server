package de.bwaldvogel.mongo.exception;

public class NSNameTooLongError extends MongoServerError {

    private static final long serialVersionUID = 7385539454918648055L;

    public NSNameTooLongError(int maxSize) {
        super( 10080 , "ns name too long, max size is " + maxSize );
    }

}
