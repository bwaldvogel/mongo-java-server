package de.bwaldvogel.mongo.exception;

import java.io.Serial;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class CannotIndexParallelArraysError extends KeyConstraintError {

    @Serial
    private static final long serialVersionUID = 1L;

    public CannotIndexParallelArraysError(Collection<String> paths) {
        super(ErrorCode.CannotIndexParallelArrays, "cannot index parallel arrays " + formatPaths(paths));
    }

    private static String formatPaths(Collection<String> paths) {
        List<String> firstTwoPaths = paths.stream()
            .map(v -> "[" + v + "]")
            .limit(2)
            .collect(Collectors.toList());

        Collections.reverse(firstTwoPaths);
        return String.join(" ", firstTwoPaths);
    }
}
