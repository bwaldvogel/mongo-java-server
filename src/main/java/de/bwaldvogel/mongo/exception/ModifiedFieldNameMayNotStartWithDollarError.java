package de.bwaldvogel.mongo.exception;


public class ModifiedFieldNameMayNotStartWithDollarError extends MongoServerError {

    private static final long serialVersionUID = 4956533382207336866L;

    public ModifiedFieldNameMayNotStartWithDollarError() {
        super(15896, "Modified field name may not start with $");
    }

}
