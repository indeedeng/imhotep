package com.indeed.flamdex.simple;

import com.google.common.base.Throwables;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.imhotep.multicache.ftgs.TermDesc;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

/**
 * @author arun.
 */
public class MultiShardFlamdexReader implements Closeable {
    private static final Logger log = Logger.getLogger(MultiShardFlamdexReader.class);
    public final FlamdexReader[] flamdexReaders;

    public MultiShardFlamdexReader(FlamdexReader[] flamdexReaders) {
        this.flamdexReaders = Arrays.copyOf(flamdexReaders, flamdexReaders.length);
    }

    public Iterator<TermDesc> intTermOffsetIterator(final String intField) throws IOException {
        final IntTermIterator[] iterators = new IntTermIterator[flamdexReaders.length];
        try {
            for (int i = 0; i < flamdexReaders.length; i++) {
                final FlamdexReader flamdexReader = flamdexReaders[i];
                iterators[i] = flamdexReader.getIntTermIterator(intField);
            }
        } catch (final Exception e) {
            Closeables2.closeAll(Arrays.asList(iterators), log);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardIntTermIterator(intField, iterators);
    }

    public Iterator<TermDesc> stringTermOffsetIterator(final String stringField) throws  IOException {
        final StringTermIterator[] iterators = new StringTermIterator[flamdexReaders.length];
        try {
            for (int i = 0; i < iterators.length; i++) {
                final FlamdexReader flamdexReader = flamdexReaders[i];
                iterators[i] = flamdexReader.getStringTermIterator(stringField);
            }
        } catch (final Exception e) {
            Closeables2.closeAll(Arrays.asList(iterators), log);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardStringTermIterator(stringField, iterators);
    }

    @Override
    public void close() {
        Closeables2.closeAll(Arrays.asList(flamdexReaders), log);
    }
}
