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

import com.indeed.flamdex.simple.HasMapCache;
import com.indeed.imhotep.io.caching.CachedFile;
import it.unimi.dsi.fastutil.objects.Object2LongArrayMap;

import java.io.File;
import java.io.IOException;
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

    private final long shardPtr;

    public NativeShard(final HasMapCache reader,
                       final long        packedTablePtr)
        throws IOException {

        final String shardDir = getDirectory(reader);

        final int numIntFields = reader.getIntFields().size();
        final int numStrFields = reader.getStringFields().size();

        final String[] intFields = reader.getIntFields().toArray(new String[numIntFields]);
        final String[] strFields = reader.getStringFields().toArray(new String[numStrFields]);

        final Map<String, Long> cached = new Object2LongArrayMap<String>();
        reader.getMapCache().getAddresses(cached);

        final String[] mappedFiles = new String[cached.size()];
        final long[]   mappedPtrs  = new long[cached.size()];
        int idx = 0;
        for (Map.Entry<String, Long> entry: cached.entrySet()) {
            mappedFiles[idx] = entry.getKey();
            mappedPtrs[idx]  = entry.getValue();
            ++idx;
        }

        try {
            shardPtr = nativeGetShard(shardDir, intFields, strFields, packedTablePtr,
                                      mappedFiles, mappedPtrs);
        }
        catch (Throwable th) {
            throw new IOException("unable to create native shard for dir:" + shardDir, th);
        }
    }

    public void close() { nativeReleaseShard(shardPtr); }

    public long getPtr() { return shardPtr; }

    public String toString() { return nativeToString(shardPtr); }

    /**
     * In some cases, FlamdexReaders contain references to paths that
     * don't exist until CachedFile does its thing. So we need to
     * force cache population before we actually do anything with the
     * shard dir.
     */
    private String getDirectory(final HasMapCache reader)
        throws IOException {
        final CachedFile cachedFile = CachedFile.create(reader.getDirectory());
        final File       cachedDir  = cachedFile.loadDirectory();
        return cachedDir.getAbsolutePath();
    }

    private native static long nativeGetShard(final String   shardDir,
                                              final String[] intFields,
                                              final String[] strFields,
                                              final long     packedTablePtr,
                                              final String[] mappedFiles,
                                              final long[]   mappedPtrs);

    private native static void nativeReleaseShard(long shardPtr);

    private native static String nativeToString(long shardPtr);
}
