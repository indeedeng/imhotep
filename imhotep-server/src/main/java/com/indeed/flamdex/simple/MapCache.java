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
