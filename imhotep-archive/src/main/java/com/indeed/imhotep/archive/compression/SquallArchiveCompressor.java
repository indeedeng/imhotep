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
