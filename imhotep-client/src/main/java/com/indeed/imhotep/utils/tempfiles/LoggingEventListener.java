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

    private void log(@Nullable final StackTraceElement[] stackTraceElements, final String format, final Object... args) {
        if (logger.isEnabledFor(level)) {
            if (stackTraceElements == null) {
                logger.log(level, String.format(format, args));
            } else {
                logger.log(level, String.format(format, args), StackTraceUtils.createThrowableWithStacktrace("Dummy throwable that hs stackTrace on temp file creation", stackTraceElements));
            }
        }
    }

    @Override
    public void removeTwice(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Try to remove temp file %s twice or more.", tempFileState.getPath());
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Try to remove temp file %s which is still referenced.", tempFileState.getPath());
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Input stream on %s is finalized without close.", tempFileState.getPath());
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Output stream on %s is finalized without close.", tempFileState.getPath());
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Temp file object for %s is finalized without removing the file.", tempFileState.getPath());
    }

    @Override
    public void expired(final TempFileState tempFileState) {
        log(tempFileState.getStackTraceElements(), "Temp file object for %s is referenced for a long time.", tempFileState.getPath());
    }
}
