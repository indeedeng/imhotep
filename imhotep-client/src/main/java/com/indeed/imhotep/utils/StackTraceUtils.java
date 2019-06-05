package com.indeed.imhotep.utils;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

public class StackTraceUtils {
    private static final Logger LOGGER = Logger.getLogger(StackTraceUtils.class);

    @Nullable
    public static StackTraceElement[] tryGetStackTrace() {
        try {
            return Thread.currentThread().getStackTrace();
        } catch (final Throwable e) {
            LOGGER.warn("Failed to get stack trace", e);
            return null;
        }
    }

    public static Throwable createThrowableWithStacktrace(final String message, final StackTraceElement[] stackTraceElements) {
        return new DummyThrowable(message, stackTraceElements);
    }

    private static class DummyThrowable extends Throwable {
        private DummyThrowable(final String message, final StackTraceElement[] stackTraceElements) {
            super(message, null, true, true);
            this.setStackTrace(stackTraceElements);
        }
    }
}
