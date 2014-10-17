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
 package com.indeed.imhotep.io;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLockInterruptionException;
import java.nio.channels.OverlappingFileLockException;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.indeed.util.core.io.Closeables2;


public class ReadLock implements Closeable {
    private static final Logger log = Logger.getLogger(ReadLock.class);

    public static ReadLock lock(Map<File, RandomAccessFile> lockFileMap, File indexDir) throws LockAquisitionException,
                                                                                       IOException {
        acquireReadLock(lockFileMap, indexDir);
        return new ReadLock(lockFileMap, indexDir);
    }

    private final Map<File, RandomAccessFile> lockFileMap;

    private final File indexDir;

    public ReadLock(final Map<File, RandomAccessFile> lockFileMap, final File indexDir) {
        this.lockFileMap = lockFileMap;
        this.indexDir = indexDir;
    }

    @Override
    public void close() throws IOException {
        synchronized (lockFileMap) {
            final RandomAccessFile randomAccessFile = lockFileMap.get(indexDir);
            Closeables2.closeQuietly(randomAccessFile, log);
            lockFileMap.remove(indexDir);
        }
    }

    private static void acquireReadLock(Map<File, RandomAccessFile> lockFileMap, File indexDir) throws IOException,
                                                                                               LockAquisitionException {
        final File writeLock = new File(indexDir, "delete.lock");
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
    }

    public static class LockAquisitionException extends Exception {

        public LockAquisitionException() {
        }
    }

    public static final class ShardDeletedException extends LockAquisitionException {

        private ShardDeletedException() {
        }
    }

    public static final class AlreadyOpenException extends LockAquisitionException {

        private AlreadyOpenException() {
        }
    }
}

