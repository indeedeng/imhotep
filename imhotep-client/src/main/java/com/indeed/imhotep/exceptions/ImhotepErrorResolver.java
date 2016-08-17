package com.indeed.imhotep.exceptions;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TempFileSizeLimitExceededException;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * Tries to resolve Imhotep errors which had their types lost in Protobuf shipping to a more structured representation
 * @author vladimir
 */
public class ImhotepErrorResolver {
    public static Exception resolve(Exception e) {
        final String error = ExceptionUtils.getRootCauseMessage(e);
        if (error.contains(ImhotepOutOfMemoryException.class.getSimpleName())) {
            return new ImhotepOverloadedException("Imhotep is overloaded with all memory in use. " +
                    "Please wait before retrying.", e);
        }

        if (error.contains(TooManySessionsException.class.getSimpleName())) {
            return new ImhotepOverloadedException("Imhotep is overloaded with too many concurrent sessions. " +
                    "Please wait before retrying.", e);
        }

        if (error.contains(com.indeed.imhotep.exceptions.UserSessionCountLimitExceededException.class.getSimpleName())) {
            return new UserSessionCountLimitExceededException("You have too many concurrent Imhotep sessions running. " +
                    "Please wait for them to complete before retrying.", e);
        }

        if (error.contains(TempFileSizeLimitExceededException.class.getSimpleName())) {
            return new FTGSLimitExceededException("The query tried to iterate over too much data. " +
                    "Please simplify the query.", e);
        }

        if (error.contains(MultiValuedFieldRegroupException.class.getSimpleName())) {
            return new MultiValuedFieldRegroupException("Query failed trying to do an Imhotep regroup on a " +
                    "multi-valued field. Grouping by a multi-valued field only works if it's the last group by and percentile/distinct are not used.", e);
        }

        if (error.contains("BitSet fields should only have term")) {
            return new StringFieldInSelectException("The query attempted to use a string field where a field with " +
                    "only numeric values is required. For example only int fields and string fields containing only " +
                    "numbers can be used in the SELECT clause.", e);
        }

        return e;
    }
}
