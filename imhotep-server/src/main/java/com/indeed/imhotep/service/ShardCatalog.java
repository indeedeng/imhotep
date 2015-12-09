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

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.io.Shard;
import com.indeed.lsmtree.core.Store;
import com.indeed.lsmtree.core.StoreBuilder;
import com.indeed.util.core.Pair;
import com.indeed.util.serialization.IntSerializer;
import com.indeed.util.serialization.LongSerializer;
import com.indeed.util.serialization.Serializer;
import com.indeed.util.serialization.StringSerializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * A persistent ShardInfo list backed by an LSM tree. We store the
 * minimum required to reconstitute ShardInfo objects on startup:
 *
 * key: (dataset, shard id)
 * value: (numDocs, version)
 *
 * We don't stash loaded metrics away since those are presumably
 * ephemeral.
 */
class ShardCatalog implements AutoCloseable {

    static final class Key extends Pair<String, String> {
        Key(String dataset, String shardId) { super(dataset, shardId); }
        public String getDataset() { return getFirst(); }
        public String getShardId() { return getSecond(); }
    }

    static final class Value extends Pair<Integer, Long> {
        Value(Integer numDocs, Long version) { super(numDocs, version); }
        public int getNumDocs() { return getFirst(); }
        public long getVersion() { return getSecond(); }
    }

    private static final IntSerializer    intSerializer   = new IntSerializer();
    private static final LongSerializer   longSerializer  = new LongSerializer();
    private static final StringSerializer strSerializer   = new StringSerializer();
    private static final KeySerializer    keySerializer   = new KeySerializer();
    private static final ValueSerializer  valueSerializer = new ValueSerializer();

    private final Store<Key, Value> store;

    ShardCatalog(File root) throws IOException {
        StoreBuilder<Key, Value> builder =
            new StoreBuilder<Key, Value>(root, keySerializer, valueSerializer);
        store = builder.build();
    }

    @Override public void close() throws IOException { store.close(); }

    Store<Key, Value> getStore() { return store; }

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
        }

        public Value read(DataInput in) throws IOException {
            final int  numDocs = intSerializer.read(in);
            final long version = longSerializer.read(in);
            return new Value(numDocs, version);
        }
    }
}
