package com.indeed.imhotep.utils;

import com.indeed.imhotep.exceptions.GenericImhotepKnownException;
import com.indeed.imhotep.exceptions.ImhotepKnownException;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.SocketTimeoutException;

/**
 * @author xweng
 */
public class ImhotepExceptionUtils {

    public static IOException buildIOExceptionFromResponse(
            final ImhotepResponse response,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final String msg = buildExceptionMessage(response, host, port, sessionId);
        return new IOException(msg);
    }

    public static ImhotepKnownException buildImhotepKnownExceptionFromResponse(
            final ImhotepResponse response,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final String msg = buildExceptionMessage(response, host, port, sessionId);
        return new GenericImhotepKnownException(msg);
    }

    public static String buildExceptionMessage(final ImhotepResponse response, final String host, final int port, @Nullable final String sessionId) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" returned error: ")
                .append(response.getExceptionStackTrace()); // stack trace string includes the type and message
        if (sessionId != null) {
            msg.append(" sessionId :").append(sessionId);
        }
        return msg.toString();
    }

    public static IOException buildExceptionAfterSocketTimeout(
            final SocketTimeoutException e,
            final String host,
            final int port,
            @Nullable final String sessionId) {
        final StringBuilder msg = new StringBuilder();
        msg.append("imhotep daemon ").append(host).append(":").append(port)
                .append(" socket timed out: ").append(e.getMessage());
        if (sessionId != null) {
            msg.append(" sessionId: ").append(sessionId);
        }

        return new IOException(msg.toString());
    }
}
