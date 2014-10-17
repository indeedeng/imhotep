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
 package com.indeed.flamdex.simple;

import com.google.common.collect.Maps;
import com.indeed.imhotep.io.caching.CachedFile;
import com.indeed.util.core.io.Closeables2;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.MMapBuffer;

import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author jplaisance
 */
public final class MapCache implements Closeable {

    private static final Logger log = Logger.getLogger(MapCache.class);

    private final Map<String, SharedReference<MMapBuffer>> mappingCache = Maps.newHashMap();

    public MapCache() {}

    public synchronized SharedReference<MMapBuffer> copyOrOpen(String filename) throws IOException {
        SharedReference<MMapBuffer> reference = mappingCache.get(filename);
        if (reference == null) {
            final File file;
            final MMapBuffer mmapBuf;
            final CachedFile cf = CachedFile.create(filename);

            file = cf.loadFile();
            mmapBuf = new MMapBuffer(file, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
            reference = SharedReference.create(mmapBuf);
            mappingCache.put(filename, reference);
        }
        return reference.copy();
    }

    @Override
    public synchronized void close() throws IOException {
        for (Map.Entry<String, SharedReference<MMapBuffer>> entry : mappingCache.entrySet()) {
            Closeables2.closeQuietly(entry.getValue(), log);
        }
    }
}
