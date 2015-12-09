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

import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;

/**
 * A persistent DatasetInfo list backed by an LSM tree. We store the minimum
 * required to reconstitute DatasetInfo objects on startup:
 *
 * key: (dataset)
 * value: (list<int fields>, list<str fields>)
 *
 * We don't stash 'shardList' since we can initialize that from ShardCatalog. We
 * don't save 'metrics' because that list is identical to 'int fields'.
 */
class DatasetCatalog implements AutoCloseable {

    static final class Value {
        final ObjectArrayList<String> intFields;
        final ObjectArrayList<String> strFields;
        Value(ObjectArrayList<String> intFields,
              ObjectArrayList<String> strFields) {
            this.intFields = intFields;
            this.strFields = strFields;
        }
    }

    private static final IntSerializer       intSerializer       = new IntSerializer();
    private static final StringSerializer    strSerializer       = new StringSerializer();
    private static final FieldListSerializer fieldListSerializer = new FieldListSerializer();
    private static final ValueSerializer     valueSerializer     = new ValueSerializer();

    private final Store<String, Value> store;

    DatasetCatalog(File root) throws IOException {
        StoreBuilder<String, Value> builder =
            new StoreBuilder<String, Value>(root, strSerializer, valueSerializer);
        store = builder.build();
    }

    @Override public void close() throws IOException { store.close(); }

    Store<String, Value> getStore() { return store; }

    private static final class FieldListSerializer
        implements Serializer<ObjectArrayList<String>> {

        public final void write(ObjectArrayList<String> fields, DataOutput out)
            throws IOException {
            intSerializer.write(fields.size(), out);
            for (String field: fields) {
                strSerializer.write(field, out);
            }
        }

        public final ObjectArrayList<String> read(DataInput in)
            throws IOException {
            final int numFields = intSerializer.read(in);
            final ObjectArrayList<String> result =
                new ObjectArrayList<String>(numFields);
            for (int count = 0; count < numFields; ++count) {
                final String field = strSerializer.read(in);
                result.add(field);
            }
            return result;
        }
    }

    private static final class ValueSerializer implements Serializer<Value> {

        public void write(Value value, DataOutput out) throws IOException {
            fieldListSerializer.write(value.intFields, out);
            fieldListSerializer.write(value.strFields, out);
        }

        public Value read(DataInput in) throws IOException {
            final ObjectArrayList<String> intFields = fieldListSerializer.read(in);
            final ObjectArrayList<String> strFields = fieldListSerializer.read(in);
            return new Value(intFields, strFields);
        }
    }
}
