package de.bwaldvogel.mongo.exception;

import java.io.Serial;

public class FailedToOptimizePipelineError extends MongoServerError {

    @Serial
    private static final long serialVersionUID = 1L;

    private static final String FAILED_TO_OPTIMIZE_MESSAGE = "Failed to optimize pipeline :: caused by :: ";

    public FailedToOptimizePipelineError(int errorCode, String message) {
        super(errorCode, FAILED_TO_OPTIMIZE_MESSAGE + message);
    }

    public FailedToOptimizePipelineError(ErrorCode errorCode, String message) {
        super(errorCode, FAILED_TO_OPTIMIZE_MESSAGE + message);
    }
}
