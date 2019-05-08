package com.indeed.imhotep.utils;

import com.google.common.base.Throwables;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.protobuf.ImhotepResponse;

/**
 * @author xweng
 */
public class ImhotepResponseUtils {
    public static ImhotepResponse newErrorResponse(final Exception e) {
        final ImhotepResponse.Builder builder = ImhotepResponse.newBuilder();
        appendErrorMessage(e, builder);
        return builder.build();
    }

    public static void appendErrorMessage(final Exception e, final ImhotepResponse.Builder builder) {
        builder
                .setExceptionType(e.getClass().getName())
                .setExceptionMessage(e.getMessage() != null ? e.getMessage() : "")
                .setExceptionStackTrace(Throwables.getStackTraceAsString(e));
        if ((e instanceof ImhotepKnownException)) {
            builder.setResponseCode(ImhotepResponse.ResponseCode.KNOWN_ERROR);
        } else {
            builder.setResponseCode(ImhotepResponse.ResponseCode.OTHER_ERROR);
        }
    }
}
