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
 package com.indeed.flamdex.simple;

import com.indeed.util.serialization.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author jsgroth
 */
public class LongPairSerializer implements Serializer<LongPair> {
    @Override
    public void write(final LongPair o, final DataOutput out) throws IOException {
        out.writeLong(o.getFirst());
        out.writeLong(o.getSecond());
    }

    @Override
    public LongPair read(final DataInput in) throws IOException {
        final long first = in.readLong();
        final long second = in.readLong();
        return new LongPair(first, second);
    }
}
