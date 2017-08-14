/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    private AcquireShared() {
    }

    private static final Logger log = Logger.getLogger(AcquireShared.class);

    public static void main(final String[] args) throws InterruptedException {
        System.out.println(System.getProperty("java.version"));
        final File testlocking = new File("testlocking");
        testlocking.mkdir();
        final Map<File, RandomAccessFile> lockFileMap = new HashMap<>();
        try {
            acquireReadLock(lockFileMap, testlocking);
        } catch (final AlreadyOpenException e) {
            System.out.println("already open");
        } catch (final LockAquisitionException e) {
            System.out.println("deleted");
        }

        try {
            acquireReadLock(lockFileMap, testlocking);
            System.out.println("should've failed");
        } catch (final AlreadyOpenException e) {
            //ignore
        } catch (final LockAquisitionException e) {
            System.out.println("deleted");
        }
        Thread.sleep(Long.MAX_VALUE);
    }

    private static void acquireReadLock(final Map<File, RandomAccessFile> lockFileMap, final File indexDir) throws LockAquisitionException {
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
                    } catch (final OverlappingFileLockException e) {
                        lockFileMap.remove(indexDir);
                        Closeables2.closeQuietly(raf, log);
                        throw Throwables.propagate(e);
                    } catch (final FileLockInterruptionException e) {
                        lockFileMap.remove(indexDir);
                        Closeables2.closeQuietly(raf, log);
                    }
                }
            }
        } catch (final IOException e) {
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
