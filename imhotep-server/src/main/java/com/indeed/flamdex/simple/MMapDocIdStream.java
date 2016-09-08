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

import com.indeed.util.core.reference.SharedReference;
import com.indeed.util.mmap.DirectMemory;
import com.indeed.util.mmap.MMapBuffer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @author jsgroth
 * @author dwahler
 */
final class MMapDocIdStream extends SimpleDocIdStream {
    private static final Logger LOG = Logger.getLogger(MMapDocIdStream.class);

    private final MapCache mapCache;

    private DirectMemory memory;
    private SharedReference<MMapBuffer> file;

    public static final int BUFFER_SIZE = 8192;

    MMapDocIdStream(MapCache mapCache) {
        this(mapCache, new byte[BUFFER_SIZE]);
    }

    MMapDocIdStream(MapCache mapCache, byte[] buffer) {
        super(buffer);
        this.mapCache = mapCache;
    }

    @Override
    protected void openFile(final Path filePath) throws IOException {
        if (file != null) file.close();
        file = mapCache.copyOrOpen(filePath);
        memory = file.get().memory();
    }

    @Override
    protected long getLength() {
        return memory.length();
    }

    @Override
    protected void readBytes(long offset) {
        memory.getBytes(offset, buffer, 0, bufferLen);
    }

    @Override
    public void close() {
        try {
            if (file != null) {
                file.close();
                file = null;
            }
        } catch (IOException e) {
            LOG.error("error closing file", e);
        }
    }
}
