package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLockInterruptionException;

/**
 * @author jplaisance
 */
public final class AcquireExclusive {

    private static final Logger log = Logger.getLogger(AcquireExclusive.class);

    public static void main(String[] args) throws InterruptedException {
        System.out.println(System.getProperty("java.version"));
        final File testlocking = new File("testlocking");
        testlocking.mkdir();
        acquireExclusive(testlocking);
        Thread.sleep(Long.MAX_VALUE);
    }

    private static RandomAccessFile acquireExclusive(File indexDir) {
        final File writeLock = new File(indexDir, "delete.lock");
        try {
            writeLock.createNewFile();
            while (true) {
                final RandomAccessFile raf = new RandomAccessFile(writeLock, "rw");
                final FileChannel channel = raf.getChannel();
                try {
                    channel.lock(0, Long.MAX_VALUE, false);
                    if (indexDir.exists()) {
                        System.out.println("lock acquired");
                        return raf;
                    }
                    Closeables2.closeQuietly(raf, log);
                    return null;
                } catch (FileLockInterruptionException e) {
                    Closeables2.closeQuietly(raf, log);
                }
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
