package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class MongoServerNotYetImplementedException extends MongoServerException {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final String ISSUES_URL = "https://github.com/bwaldvogel/mongo-java-server/issues/";

    public MongoServerNotYetImplementedException(int gitHubIssueNumber, String prefix) {
        super(prefix + " is not yet implemented. See " + ISSUES_URL + gitHubIssueNumber);
    }

}
