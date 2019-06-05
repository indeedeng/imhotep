package com.indeed.imhotep.utils.tempfiles;

import java.util.concurrent.atomic.AtomicInteger;

public class CountingEventListener implements EventListener {
    final AtomicInteger removeTwiceCount = new AtomicInteger(0);
    final AtomicInteger removeReferencedFileCount = new AtomicInteger(0);
    final AtomicInteger didNotCloseInputStreamCount = new AtomicInteger(0);
    final AtomicInteger didNotCloseOutputStreamCount = new AtomicInteger(0);
    final AtomicInteger didNotRemoveTempFileCount = new AtomicInteger(0);
    final AtomicInteger expiredCount = new AtomicInteger(0);

    int getSum() {
        return removeTwiceCount.get()
                + removeReferencedFileCount.get()
                + didNotCloseInputStreamCount.get()
                + didNotCloseOutputStreamCount.get()
                + didNotRemoveTempFileCount.get()
                + expiredCount.get();
    }

    @Override
    public void removeTwice(final TempFileState tempFileState) {
        removeTwiceCount.incrementAndGet();
    }

    @Override
    public void removeReferencedFile(final TempFileState tempFileState) {
        removeReferencedFileCount.incrementAndGet();
    }

    @Override
    public void didNotCloseInputStream(final TempFileState tempFileState) {
        didNotCloseInputStreamCount.incrementAndGet();
    }

    @Override
    public void didNotCloseOutputStream(final TempFileState tempFileState) {
        didNotCloseOutputStreamCount.incrementAndGet();
    }

    @Override
    public void didNotRemoveTempFile(final TempFileState tempFileState) {
        didNotRemoveTempFileCount.incrementAndGet();
    }

    @Override
    public void expired(final TempFileState tempFileState) {
        expiredCount.incrementAndGet();
    }
}
