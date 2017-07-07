/*
 * Copyright (C) 2016 Indeed Inc.
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
package com.indeed.imhotep.local;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * A fair bit of our native code makes use of a Shard object, which
 * manages term files, doc files and pointers to fields that have
 * already been inverted in Javaland. To build one of these critters,
 * you need a FlamdexReader that implements HasMapCache. This class
 * does two basic things:
 *
 *   - it expresses lifecycle management of a native Shard object in
 *     Java Closeable fashion
 *
 *   - it converts relevant members of the FlamdexReader into a form
 *     convenient for purposes of constructing a native Shard object,
 *     elminating the need for elaborate JNI field access gymnastics
 *     inside the native code
 */
class NativeShard implements AutoCloseable {

    private long shardPtr;

    public NativeShard(final SimpleFlamdexReader reader,
                       final long                packedTablePtr)
        throws IOException {

        final Path shardDir = getDirectory(reader);

        final Map<Path, Long> cached = new Object2LongArrayMap<>();
        reader.getMapCache().getAddresses(cached);

        final String[] mappedFiles = new String[cached.size()];
        final long[]   mappedPtrs  = new long[cached.size()];
        int idx = 0;
        for (final Map.Entry<Path, Long> entry: cached.entrySet()) {
            mappedFiles[idx] = entry.getKey().toFile().toString();
            mappedPtrs[idx]  = entry.getValue();
            ++idx;
        }

        // here we take Path and get the string path to the locally cached shard files
        // this is okay because the cached files are guaranteed to be present for the lifetime of the mapcache
        try {
            shardPtr = nativeGetShard(shardDir.toFile().toString(), packedTablePtr, mappedFiles, mappedPtrs);
        }
        catch (final Throwable e) {
            throw new IOException("unable to create native shard for dir:" + shardDir, e);
        }
    }

    @Override
    public void close() {
        try {
            nativeReleaseShard(shardPtr);
        } finally {
            shardPtr = 0;
        }
    }

    public long getPtr() { return shardPtr; }

    public String toString() { return nativeToString(shardPtr); }

    /**
     * In some cases, FlamdexReaders contain references to paths that
     * don't exist until CachedFile does its thing. So we need to
     * force cache population before we actually do anything with the
     * shard dir.
     */
    private Path getDirectory(final FlamdexReader reader)
        throws IOException {
        final Path dir = reader.getDirectory();
        if(dir == null) {
            throw new IllegalArgumentException("NativeShard: FlamdexReader is expected to have non-null path.");
        }
        if(!Files.isDirectory(dir)) {
            throw new RuntimeException("NativeShard: " + dir.toString() + " is not a directory.");
        }
        return dir;
    }

    private static native long nativeGetShard(final String   shardDir,
                                              final long     packedTablePtr,
                                              final String[] mappedFiles,
                                              final long[]   mappedPtrs);

    private static native void nativeReleaseShard(long shardPtr);

    private static native String nativeToString(long shardPtr);
}
