package de.bwaldvogel.mongo.exception;

import de.bwaldvogel.mongo.backend.memory.index.Index;

public class DuplicateKeyException extends KeyConstraintException {

    private static final long serialVersionUID = 7972569155402894456L;

    public DuplicateKeyException(Index index , Object value) {
        super( 11000 , "duplicate key error index: " + index.getName() + "  dup key: " + value );
    }

}
