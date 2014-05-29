package com.indeed.imhotep.service;

import com.google.common.base.Throwables;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.OverlappingFileLockException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jplaisance
 */
public final class AcquireShared {

    private static final Logger log = Logger.getLogger(AcquireShared.class);

    public static void main(String[] args) throws InterruptedException {
        System.out.println(System.getProperty("java.version"));
        final File testlocking = new File("testlocking");
        testlocking.mkdir();
        final Map<File, RandomAccessFile> lockFileMap = new HashMap<File, RandomAccessFile>();
        try {
            acquireReadLock(lockFileMap, testlocking);
        } catch (AlreadyOpenException e) {
            System.out.println("already open");
        } catch (LockAquisitionException e) {
            System.out.println("deleted");
        }

        try {
            acquireReadLock(lockFileMap, testlocking);
            System.out.println("should've failed");
        } catch (AlreadyOpenException e) {
            //ignore
        } catch (LockAquisitionException e) {
            System.out.println("deleted");
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void acquireReadLock(Map<File, RandomAccessFile> lockFileMap, File indexDir) throws LockAquisitionException {
        final File writeLock = new File(indexDir, "delete.lock");
        try {
            writeLock.createNewFile();
            while (true) {
                synchronized (lockFileMap) {
                    RandomAccessFile raf = lockFileMap.get(indexDir);
                    if (raf == null) {
                        raf = new RandomAccessFile(writeLock, "r");
                        lockFileMap.put(indexDir, raf);
                    } else {
                        throw new AlreadyOpenException();
                    }

                    final FileChannel channel = raf.getChannel();
                    try {
                        channel.lock(0, Long.MAX_VALUE, true);
                        if (indexDir.exists()) {
                            System.out.println("lock acquired");
                            return;
                        }
                        lockFileMap.remove(indexDir);
                        Closeables2.closeQuietly(raf, log);
                        throw new ShardDeletedException();
                    } catch (OverlappingFileLockException e) {
                        lockFileMap.remove(indexDir);
                        Closeables2.closeQuietly(raf, log);
                        throw Throwables.propagate(e);
                    } catch (FileLockInterruptionException e) {
                        lockFileMap.remove(indexDir);
                        Closeables2.closeQuietly(raf, log);
                    }
                }
            }
        } catch (IOException e) {
            throw new ShardDeletedException();
        }
    }

    private static class LockAquisitionException extends Exception {

        public LockAquisitionException() {
        }
    }

    private static final class ShardDeletedException extends LockAquisitionException {

        private ShardDeletedException() {
        }
    }

    private static final class AlreadyOpenException extends LockAquisitionException {

        private AlreadyOpenException() {
        }
    }
}
