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
package com.indeed.imhotep.service;

import com.indeed.lsmtree.core.StorageType;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.compress.SnappyCodec;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.StringSerializer;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * ShardStore provides a way to serialize a ShardMap to an LSMTree.
 *
 * key: (dataset, shardId)
 * value: (shardDir, numDocs, version, [int fields], [str fields])
 */
class ShardStore implements AutoCloseable {

    private static final IntSerializer        intSerializer     = new IntSerializer();
    private static final KeySerializer        keySerializer     = new KeySerializer();
    private static final LongSerializer       longSerializer    = new LongSerializer();
    private static final StringListSerializer strListSerializer = new StringListSerializer();
    private static final StringSerializer     strSerializer     = new StringSerializer();
    private static final ValueSerializer      valueSerializer   = new ValueSerializer();

    private final Store<Key, Value> store;

    ShardStore(final Path root) throws IOException {
        final StoreBuilder<Key, Value> builder =
            new StoreBuilder<>(root.toFile(), keySerializer, valueSerializer);
        builder.setCodec(new SnappyCodec());
        builder.setStorageType(StorageType.BLOCK_COMPRESSED);
        store = builder.build();
    }

    @Override public void close() throws IOException {
        store.close();
    }

    // Close store and wait for all writings to complete.
    // TODO: maybe add this fuctionality to close method?
    void safeClose() throws IOException, InterruptedException {
        store.close();
        store.waitForCompactions();
    }

    Iterator<Store.Entry<Key, Value>> iterator() throws IOException  {
        return store.iterator();
    }

    boolean containsKey(final Key key) throws IOException { return store.containsKey(key); }

    void put(final Key key, final Value value) throws IOException { store.put(key, value); }

    void delete(final Key key) throws IOException { store.delete(key); }

    void sync() throws IOException { store.sync(); }

    /** Attempt to safely delete a ShardStore. Since the failure mode for
     * deleting the wrong directory can be extreme, this code tries to
     * heuristically confirm that storeDir contains an LSM tree by looking for
     * telltale files within it.
     */
    static void deleteExisting(final Path shardStoreDir) throws IOException {
        if (shardStoreDir == null) {
            return;
        }

        if (Files.notExists(shardStoreDir)) {
            return;
        }
        if (!Files.isDirectory(shardStoreDir)) {
            return;
        }

        boolean foundLatest = false;
        boolean foundData = false;
        try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(shardStoreDir)) {
            for (final Path child : dirStream) {
                if (child.getFileName().toString().equals("latest")) {
                    foundLatest = true;
                }
                if (child.getFileName().toString().equals("data")) {
                    foundData = true;
                }
            }
        }

        if (foundLatest && foundData) {
            PosixFileOperations.rmrf(shardStoreDir);
        }
    }

    public static final class Key
        implements Comparable<Key> {

        private final String dataset;
        private final String shardId;

        Key(final String dataset, final String shardId) {
            this.dataset = dataset;
            this.shardId = shardId;
        }

        public String getDataset() { return dataset; }
        public String getShardId() { return shardId; }

        @Override public int hashCode() {
            int result = 1;
            result = result * 31 + getDataset().hashCode();
            result = result * 31 + getShardId().hashCode();
            return result;
        }

        @Override public boolean equals(final Object otherObject) {
            if (this == otherObject) {
                return true;
            }
            if (this.getClass() != otherObject.getClass()) {
                return false;
            }
            final Key other = (Key) otherObject;
            return getDataset().equals(other.getDataset()) && getShardId().equals(other.getShardId());
        }

        @Override public int compareTo(@Nonnull final Key other) {
            int result = getDataset().compareTo(other.getDataset());
            if (result != 0) {
                return result;
            }
            result = getShardId().compareTo(other.getShardId());
            return result;
        }

        @Override public String toString() {
            return "{ dataset: " + dataset + ", shardId: " + shardId + " }";
        }
    }

    public static final class Value {

        private final String       shardDir; // relative to canonical shard dir
        private final int          numDocs;
        private final long         version;
        private final List<String> intFields;
        private final List<String> strFields;

        Value(final String       shardDir,
              final Integer      numDocs,
              final Long         version,
              final List<String> intFields,
              final List<String> strFields) {
            this.shardDir  = shardDir;
            this.numDocs   = numDocs;
            this.version   = version;
            this.intFields = intFields;
            this.strFields = strFields;
            Collections.sort(intFields);
            Collections.sort(strFields);
        }

        public String getShardDir() { return shardDir; }
        public int     getNumDocs() { return numDocs;  }
        public long    getVersion() { return version;  }

        public List<String> getIntFields() { return intFields; }
        public List<String> getStrFields() { return strFields; }

        @Override public int hashCode() {
            int result = 1;
            result = result * 31 + shardDir.hashCode();
            result = result * 31 + numDocs;
            result = result * 31 + Long.valueOf(version).hashCode();
            return result;
        }

        @Override public boolean equals(final Object otherObject) {
            if (this == otherObject) {
                return true;
            }
            if (otherObject.getClass() != Value.class) {
                return false;
            }
            final Value other = (Value) otherObject;
            return shardDir.equals(other.shardDir) &&
                    numDocs == other.numDocs &&
                    version == other.version &&
                    intFields.equals(other.intFields) &&
                    strFields.equals(other.strFields);
        }

        @Override public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append(" shardDir: ").append(shardDir).append(", ");
            sb.append(" numDocs: ").append(numDocs).append(", ");
            sb.append(" version: ").append(version).append(", ");
            sb.append(" intFields: [ ");
            append(sb, intFields);
            sb.append(" ] strFields: [ ");
            append(sb, strFields);
            sb.append(" ] }");
            return sb.toString();
        }

        private static <T> void append(final StringBuilder sb, final List<T> items) {
            final Iterator<T> it = items.iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                if (it.hasNext()) {
                    sb.append(", ");
                }
            }
        }
    }

    private static final class KeySerializer implements Serializer<Key> {

        public void write(final Key key, final DataOutput out) throws IOException {
            strSerializer.write(key.getDataset(), out);
            strSerializer.write(key.getShardId(), out);
        }

        public Key read(final DataInput in) throws IOException {
            final String dataset = strSerializer.read(in);
            final String shard   = strSerializer.read(in);
            return new Key(dataset, shard);
        }
    }

    private static final class ValueSerializer implements Serializer<Value> {

        public void write(final Value value, final DataOutput out) throws IOException {
            strSerializer.write(value.getShardDir(), out);
            intSerializer.write(value.getNumDocs(), out);
            longSerializer.write(value.getVersion(), out);
            strListSerializer.write(value.getIntFields(), out);
            strListSerializer.write(value.getStrFields(), out);
        }

        public Value read(final DataInput in) throws IOException {
            final String shardDir = strSerializer.read(in);
            final int    numDocs  = intSerializer.read(in);
            final long   version  = longSerializer.read(in);
            final List<String> intFields = strListSerializer.read(in);
            final List<String> strFields = strListSerializer.read(in);
            return new Value(shardDir, numDocs, version, intFields, strFields);
        }
    }

    private static final class StringListSerializer
        implements Serializer<List<String>> {

        public void write(final List<String> values, final DataOutput out)
            throws IOException {
            intSerializer.write(values.size(), out);
            for (final String field: values) {
                strSerializer.write(field, out);
            }
        }

        public List<String> read(final DataInput in)
            throws IOException {
            final int numValues = intSerializer.read(in);
            final List<String> result = new ObjectArrayList<>(numValues);
            for (int count = 0; count < numValues; ++count) {
                final String field = strSerializer.read(in);
                result.add(field);
            }
            return result;
        }
    }
}
