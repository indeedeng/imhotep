/*
 * Copyright (C) 2015 Indeed Inc.
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

import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.StringSerializer;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A persistent map of Shards backed by an LSM tree.
 *
 * key: (dataset, shard id)
 * value: (numDocs, version, [int fields], [str fields])
 *
 * We don't stash loaded metrics away since those are presumably
 * ephemeral.
 */
class ShardStore implements AutoCloseable {

    private static final Logger log = Logger.getLogger(ShardInfoStore.class);

    private static final IntSerializer        intSerializer     = new IntSerializer();
    private static final KeySerializer        keySerializer     = new KeySerializer();
    private static final LongSerializer       longSerializer    = new LongSerializer();
    private static final StringListSerializer strListSerializer = new StringListSerializer();
    private static final StringSerializer     strSerializer     = new StringSerializer();
    private static final ValueSerializer      valueSerializer   = new ValueSerializer();

    private final Store<Key, Value> store;

    ShardStore(File root) throws IOException {
        StoreBuilder<Key, Value> builder =
            new StoreBuilder<Key, Value>(root, keySerializer, valueSerializer);
        store = builder.build();
    }

    @Override public void close() throws IOException {
        store.close();
    }

    Iterator<Store.Entry<Key, Value>> iterator()
        throws IOException  {
        return store.iterator();
    }

    void put(Key key, Value value) throws IOException { store.put(key, value); }

    static public final class Key
        implements Comparable<Key> {

        private final String dataset;
        private final String shardId;

        Key(String dataset, String shardId) {
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

        @Override public boolean equals(Object otherObject) {
            if (this == otherObject) return true;
            final Key other = (Key) otherObject;
            if (!getDataset().equals(other.getDataset())) return false;
            if (!getShardId().equals(other.getShardId())) return false;
            return true;
        }

        @Override public int compareTo(Key other) {
            int result = getDataset().compareTo(other.getDataset());
            if (result != 0) return result;
            result = getShardId().compareTo(other.getShardId());
            return result;
        }

        @Override public String toString() {
            return "{ dataset: " + dataset + ", shardId: " + shardId + " }";
        }
    }

    static public final class Value {

        private final int          numDocs;
        private final long         version;
        private final List<String> intFields;
        private final List<String> strFields;

        Value(Integer      numDocs,
              Long         version,
              List<String> intFields,
              List<String> strFields) {
            this.numDocs   = numDocs;
            this.version   = version;
            this.intFields = intFields;
            this.strFields = strFields;
            Collections.sort(intFields);
            Collections.sort(strFields);
        }

        public int  getNumDocs() { return numDocs; }
        public long getVersion() { return version; }

        public List<String> getIntFields() { return intFields; }
        public List<String> getStrFields() { return strFields; }

        @Override public int hashCode() {
            int result = 1;
            result = result * 31 + numDocs;
            result = result * 31 + Long.valueOf(version).hashCode();
            return result;
        }

        @Override public boolean equals(Object otherObject) {
            if (this == otherObject) return true;
            final Value other = (Value) otherObject;
            if (numDocs != other.numDocs) return false;
            if (version != other.version) return false;
            if (!intFields.equals(other.intFields)) return false;
            if (!strFields.equals(other.strFields)) return false;
            return true;
        }

        @Override public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            sb.append(" numDocs: " + numDocs + ", ");
            sb.append(" version: " + version + ", ");
            sb.append(" intFields: [ ");
            append(sb, intFields);
            sb.append(" ] strFields: [ ");
            append(sb, strFields);
            sb.append(" ] }");
            return sb.toString();
        }

        private static <T> void append(StringBuilder sb, List<T> items) {
            Iterator<T> it = items.iterator();
            while (it.hasNext()) {
                sb.append(it.next().toString());
                if (it.hasNext()) sb.append(", ");
            }
        }
    }

    private static final class KeySerializer implements Serializer<Key> {

        public void write(Key key, DataOutput out) throws IOException {
            strSerializer.write(key.getDataset(), out);
            strSerializer.write(key.getShardId(), out);
        }

        public Key read(DataInput in) throws IOException {
            final String dataset = strSerializer.read(in);
            final String shard   = strSerializer.read(in);
            return new Key(dataset, shard);
        }
    }

    private static final class ValueSerializer implements Serializer<Value> {

        public void write(Value value, DataOutput out) throws IOException {
            intSerializer.write(value.getNumDocs(), out);
            longSerializer.write(value.getVersion(), out);
            strListSerializer.write(value.getIntFields(), out);
            strListSerializer.write(value.getStrFields(), out);
        }

        public Value read(DataInput in) throws IOException {
            final int  numDocs = intSerializer.read(in);
            final long version = longSerializer.read(in);
            final List<String> intFields = strListSerializer.read(in);
            final List<String> strFields = strListSerializer.read(in);
            return new Value(numDocs, version, intFields, strFields);
        }
    }

    private static final class StringListSerializer
        implements Serializer<List<String>> {

        public final void write(List<String> values, DataOutput out)
            throws IOException {
            intSerializer.write(values.size(), out);
            for (String field: values) {
                strSerializer.write(field, out);
            }
        }

        public final List<String> read(DataInput in)
            throws IOException {
            final int numValues = intSerializer.read(in);
            final ObjectArrayList<String> result =
                new ObjectArrayList<String>(numValues);
            for (int count = 0; count < numValues; ++count) {
                final String field = strSerializer.read(in);
                result.add(field);
            }
            return result;
        }
    }
}
