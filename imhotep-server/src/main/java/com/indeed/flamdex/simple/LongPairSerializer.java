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
    public void write(LongPair o, DataOutput out) throws IOException {
        out.writeLong(o.getFirst());
        out.writeLong(o.getSecond());
    }

    @Override
    public LongPair read(DataInput in) throws IOException {
        final long first = in.readLong();
        final long second = in.readLong();
        return new LongPair(first, second);
    }
}
