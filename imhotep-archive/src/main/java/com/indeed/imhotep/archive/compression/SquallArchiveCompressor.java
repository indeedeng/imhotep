package com.indeed.imhotep.archive.compression;

import com.google.common.collect.ImmutableMap;
import com.indeed.util.compress.CompressionCodec;
import com.indeed.util.compress.CompressionInputStream;
import com.indeed.util.compress.CompressionOutputStream;
import com.indeed.util.compress.SnappyCodec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * @author jsgroth
 *
 * Note: Trying to use LZO compression will throw a NoClassDefFoundError if your downstream project does not have
 * an explicit dependency on hadoop-lzo and a RuntimeException if the native-lzo library cannot be loaded.
 */
public enum SquallArchiveCompressor {
    NONE("none") {
        @Override
        public CompressionInputStream newInputStream(InputStream is) throws IOException {
            return new NoCompressionInputStream(is);
        }
        @Override
        public CompressionOutputStream newOutputStream(OutputStream os) throws IOException {
            return new NoCompressionOutputStream(os);
        }
    },
    GZIP("gzip") {
        @Override
        public CompressionInputStream newInputStream(InputStream is) throws IOException {
            return new GzipCompressionInputStream(is);
        }
        @Override
        public CompressionOutputStream newOutputStream(OutputStream os) throws IOException {
            return new GzipCompressionOutputStream(os);
        }
    },
    SNAPPY("snappy") {
        @Override
        public CompressionInputStream newInputStream(InputStream is) throws IOException {
            return newSnappyCodec().createInputStream(is);
        }
        @Override
        public CompressionOutputStream newOutputStream(OutputStream os) throws IOException {
            return newSnappyCodec().createOutputStream(os);
        }

        private CompressionCodec newSnappyCodec() {
            final SnappyCodec codec = new SnappyCodec();
            return codec;
        }
    };

    private static final Map<String, SquallArchiveCompressor> lookup;
    static {
        final ImmutableMap.Builder<String, SquallArchiveCompressor> builder = ImmutableMap.builder();
        for (final SquallArchiveCompressor compressor : values()) {
            builder.put(compressor.key, compressor);
        }
        lookup = builder.build();
    }

    private final String key;

    SquallArchiveCompressor(String key) {
        this.key = key;
    }

    public abstract CompressionInputStream newInputStream(InputStream is) throws IOException;

    public abstract CompressionOutputStream newOutputStream(OutputStream os) throws IOException;

    public String getKey() {
        return key;
    }

    public static SquallArchiveCompressor fromKey(String key) {
        if (!lookup.containsKey(key)) {
            throw new IllegalArgumentException("invalid key: " + key);
        }
        return lookup.get(key);
    }
}
