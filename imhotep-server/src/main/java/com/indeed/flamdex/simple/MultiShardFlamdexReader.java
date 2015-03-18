package com.indeed.flamdex.simple;

import com.google.common.base.Throwables;
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
    public final SimpleFlamdexReader[] simpleFlamdexReaders;
    private final int[] ids;

    public MultiShardFlamdexReader(SimpleFlamdexReader[] simpleFlamdexReaders, int[] ids) {
        this.simpleFlamdexReaders = Arrays.copyOf(simpleFlamdexReaders, simpleFlamdexReaders.length);
        this.ids = ids;
    }

    public Iterator<TermDesc> intTermOffsetIterator(final String intField) throws IOException {
        final SimpleIntTermIterator[] iterators = new SimpleIntTermIterator[ids.length];
        try {
            for (int i = 0; i < simpleFlamdexReaders.length; i++) {
                final SimpleFlamdexReader flamdexReader = simpleFlamdexReaders[i];
                iterators[i] = flamdexReader.getIntTermIterator(intField);
            }
        } catch (final Exception e) {
            Closeables2.closeAll(Arrays.asList(iterators), log);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardIntTermIterator(intField, iterators, ids);
    }

    public Iterator<TermDesc> stringTermOffsetIterator(final String stringField) throws  IOException {
        final SimpleStringTermIterator[] iterators = new SimpleStringTermIterator[ids.length];
        try {
            for (int i = 0; i < iterators.length; i++) {
                final SimpleFlamdexReader flamdexReader = simpleFlamdexReaders[i];
                iterators[i] = flamdexReader.getStringTermIterator(stringField);
            }
        } catch (final Exception e) {
            Closeables2.closeAll(Arrays.asList(iterators), log);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardStringTermIterator(stringField, iterators, ids);
    }

    @Override
    public void close() {
        Closeables2.closeAll(Arrays.asList(simpleFlamdexReaders), log);
    }
}
