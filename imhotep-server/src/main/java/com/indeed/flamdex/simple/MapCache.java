/*
 * Copyright (C) 2018 Indeed Inc.
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

import com.indeed.imhotep.RaceCache;
import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Map;

/**
 * @author jplaisance
 */
public class MapCache implements Closeable {
    private static final Logger log = Logger.getLogger(MapCache.class);

    private final RaceCache<Path, SharedReference<MMapBuffer>, IOException> mappingCache = RaceCache.create(MapCache::open);

    public MapCache() {}

    public SharedReference<MMapBuffer> copyOrOpen(final Path path) throws IOException {
        return mappingCache.getOrLoad(path).copy();
    }

    private static SharedReference<MMapBuffer> open(final Path path) throws IOException {
        final MMapBuffer mmapBuf = new MMapBuffer(path, FileChannel.MapMode.READ_ONLY, ByteOrder.LITTLE_ENDIAN);
        return SharedReference.create(mmapBuf);
    }

    @Override
    public synchronized void close() throws IOException {
        mappingCache.close();
    }

    /** !@# This is something of a temporary hack. We need to pass the addresses
        of mmapped files down through JNI code, so that we don't have to mmap
        them redundantly there. This pokes something of a hole in the MapCache
        abstraction, but it's a tiny leak. */
    public synchronized void getAddresses(final Map<Path, Long> result) {
        throw new UnsupportedOperationException();
    }
}
