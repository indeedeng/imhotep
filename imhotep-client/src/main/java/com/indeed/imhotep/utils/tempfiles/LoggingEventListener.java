package com.indeed.imhotep.utils.tempfiles;

import com.indeed.imhotep.utils.StackTraceUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;

public class LoggingEventListener implements EventListener {
    private final Level level;
    private final Logger logger;

    public LoggingEventListener(final Level level, final Logger logger) {
        this.level = level;
        this.logger = logger;
    }

    private void log(@Nullable final StackTraceElement[] stackTraceElements, final String message, final TempFileState tempFileState) {
        if (!logger.isEnabledFor(level)) {
            return;
        }
        @Nullable final StackTraceElement[] currentStacktrace = StackTraceUtils.tryGetStackTrace();
        @Nullable final Throwable throwableOnCreation = (stackTraceElements == null) ? null : StackTraceUtils.createThrowableWithStacktrace("Dummy throwable that has stack trace on temp file creation", stackTraceElements);
        @Nullable final Throwable throwable;
        if (currentStacktrace != null) {
            throwable = StackTraceUtils.createThrowableWithStacktrace("Dummy throwable that has current stack trace", currentStacktrace, throwableOnCreation);
        } else {
            throwable = null;
        }
        if (throwable == null) {
            logger.log(level, message + " State: " + tempFileState.toString());
        } else {
            logger.log(level, message + " State: " + tempFileState.toString(), throwable);
        }
    }

    @Override
    public void removeTwice(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Try to remove a temp file twice or more.", tempFileState);
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Try to remove a temp file which is still referenced.", tempFileState);
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "An input stream on a temp file is finalized without close.", tempFileState);
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "An output stream on a temp file is finalized without close.", tempFileState);
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "A temp file object is finalized without removing the file.", tempFileState);
    }

    @Override
    public void expired(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "A temp file object has opened i/o stream or not deleted for a long time.", tempFileState);
    }
}
