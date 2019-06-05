package com.indeed.imhotep.utils.tempfiles;

/**
 * These method will be called when some illegal but recoverable event has been detected.
 * If you throw an exception, the illegal state won't be recovered automatically.
 */
public interface EventListener {
    void removeTwice(final TempFileState tempFileState);
    void removeReferencedFile(final TempFileState tempFileState);
    void didNotCloseInputStream(final TempFileState tempFileState);
    void didNotCloseOutputStream(final TempFileState tempFileState);
    void didNotRemoveTempFile(final TempFileState tempFileState);
    /**
     * TempFile or it's i/o stream has been strongly referenced a long time.
     */
    void expired(final TempFileState tempFileState);
}
