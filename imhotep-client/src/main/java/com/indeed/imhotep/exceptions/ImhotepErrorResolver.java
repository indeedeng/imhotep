/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.exceptions;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.TempFileSizeLimitExceededException;
import com.indeed.imhotep.io.WriteLimitExceededException;
import org.apache.commons.lang.exception.ExceptionUtils;

/**
 * Tries to resolve Imhotep errors which had their types lost in Protobuf shipping to a more structured representation
 * @author vladimir
 */
public class ImhotepErrorResolver {
    private ImhotepErrorResolver() {
    }

    public static Exception resolve(final Exception e) {
        final String error = ExceptionUtils.getRootCauseMessage(e);
        if (error.contains(ImhotepOutOfMemoryException.class.getSimpleName())) {
            return new ImhotepOverloadedException("Imhotep is overloaded with all memory in use. " +
                    "Please wait before retrying.", e);
        }

        if (error.contains(TooManySessionsException.class.getSimpleName())) {
            return new ImhotepOverloadedException("Imhotep is overloaded with too many concurrent sessions. " +
                    "Please wait before retrying.", e);
        }

        if (error.contains(TempFileSizeLimitExceededException.class.getSimpleName()) ||
                error.contains(WriteLimitExceededException.class.getSimpleName())) {
            return new FTGSLimitExceededException("The query tried to iterate over too much data. " +
                    "Please simplify the query.", e);
        }

        if (error.contains(MultiValuedFieldRegroupException.class.getSimpleName())) {
            return new MultiValuedFieldRegroupException("Query failed trying to do an Imhotep regroup on a " +
                    "multi-valued field. Grouping by a multi-valued field only works if it's the last group by and percentile/distinct are not used.", e);
        }

        if (error.contains(RegexTooComplexException.class.getSimpleName())) {
            return new RegexTooComplexException("The provided regex is too complex. " +
                    "Please replace expressions like '.*A.*|.*B.*|.*C.*' with '.*(A|B|C).*'", e);
        }

        if (error.contains("BitSet fields should only have term")) {
            return new StringFieldInSelectException("The query attempted to use a string field where a field with " +
                    "only numeric values is required. For example only int fields and string fields containing only " +
                    "numbers can be used in the SELECT clause.", e);
        }

        if (error.contains("there does not exist a session with id")) {
            return new QueryCancelledException("The query was cancelled during execution", e);
        }

        if (error.contains(NonNumericFieldException.class.getSimpleName())) {
            return new NonNumericFieldException("Query failed trying to iterate over integers in a string field which " +
                    "contained non-numeric string terms. Likely cause is inconsistent type (string/int) for the same " +
                    "field in different shards. That can be be fixed in the index builder by setting an explicit field " +
                    "type and rebuilding old shards where it doesn't match.", e);
        }

        return e;
    }
}
