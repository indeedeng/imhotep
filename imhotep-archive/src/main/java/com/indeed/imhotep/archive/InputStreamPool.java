package com.indeed.imhotep.archive;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Maintains a pool of FSDataInputStreams corresponding to files that are likely to
 * be used multiple times. Not thread-safe.
 *
 * @author dwahler
 */
public class InputStreamPool implements Closeable {
    private static final Logger LOG = Logger.getLogger(InputStreamPool.class);

    private final FileSystem fs;

    // invariant: any StreamWrapper returned by get() is not present in this map until its close() method is called
    private final Map<Path, FSDataInputStream> streams;

    public InputStreamPool(final FileSystem fs) {
        this.fs = fs;
        streams = new HashMap<>();
    }

    public FSDataInputStream get(final Path path) throws IOException {
        final FSDataInputStream innerStream;

        final FSDataInputStream streamFromPool = streams.get(path);
        if (streamFromPool == null) {
            // no available stream, create a new one
            innerStream = fs.open(path);
        } else {
            // we already have a stream open, return it and remove it from the pool
            innerStream = streamFromPool;
            streams.remove(path);
        }

        return new StreamWrapper(path, innerStream);
    }

    public void close() {
        for (Map.Entry<Path, FSDataInputStream> entry : streams.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                LOG.error("Error closing input stream for file " + entry.getKey(), e);
            }
        }
    }

    private class StreamWrapper extends FSDataInputStream {
        private final Path path;

        StreamWrapper(final Path path, final InputStream in) {
            super(in);
            this.path = path;
        }

        @Override
        public void close() throws IOException {
            if (!streams.containsKey(path)) {
                // return this stream to the pool
                streams.put(path, StreamWrapper.this);
            } else {
                // there's already another stream in the pool for this file, so close this one
                super.close();
            }
        }
    }
}
