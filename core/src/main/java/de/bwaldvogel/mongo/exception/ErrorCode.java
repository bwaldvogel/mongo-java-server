package de.bwaldvogel.mongo.exception;

public enum ErrorCode {
    BadValue(2),
    FailedToParse(9),
    TypeMismatch(14),
    IllegalOperation(20),
    IndexNotFound(27),
    PathNotViable(28),
    ConflictingUpdateOperators(40),
    CursorNotFound(43),
    NamespaceExists(48),
    DollarPrefixedFieldName(52),
    InvalidIdField(53),
    CommandNotFound(59),
    ImmutableField(66),
    InvalidOptions(72),
    InvalidNamespace(73),
    IndexKeySpecsConflict(86),
    CannotIndexParallelArrays(171),
    ConversionFailure(241),
    DuplicateKey(11000),
    MergeStageNoMatchingDocument(13113),

    _15998(15998) {
        @Override
        public String getName() {
            return "Location" + getValue();
        }
    },
    _34471(34471) {
        @Override
        public String getName() {
            return "Location" + getValue();
        }
    },
    _40353(40353) {
        @Override
        public String getName() {
            return "Location" + getValue();
        }
    },
    _40390(40390) {
        @Override
        public String getName() {
            return "Location" + getValue();
        }
    },
    ;

    private final int id;

    ErrorCode(int id) {
        this.id = id;
    }

    public int getValue() {
        return id;
    }

    public String getName() {
        return name();
    }
}
