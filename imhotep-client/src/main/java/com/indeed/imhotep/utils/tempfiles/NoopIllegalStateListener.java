package com.indeed.imhotep.utils.tempfiles;

public class NoopIllegalStateListener implements EventListener {
    @Override
    public void removeTwice(final TempFileState tempFileState) {
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
    }

    @Override
    public void expired(final TempFileState tempFileState) {
    }
}
