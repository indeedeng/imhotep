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
