package com.indeed.imhotep.local;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import com.indeed.flamdex.simple.SimpleIntTermIterator;
import com.indeed.flamdex.simple.SimpleStringTermIterator;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author arun.
 */
class MultiShardFlamdexReader {
    private static final Logger log = Logger.getLogger(MultiShardFlamdexReader.class);
    public final SimpleFlamdexReader[] simpleFlamdexReaders;

    MultiShardFlamdexReader(SimpleFlamdexReader[] simpleFlamdexReaders) {
        this.simpleFlamdexReaders = Arrays.copyOf(simpleFlamdexReaders, simpleFlamdexReaders.length);
    }

    MultiShardIntTermIterator intTermOffsetIterator(final String intField) throws IOException {
        final List<SimpleIntTermIterator> iterators = Lists.newArrayListWithCapacity(simpleFlamdexReaders.length);
        try {
            for (int i = 0; i < simpleFlamdexReaders.length; i++) {
                final SimpleFlamdexReader flamdexReader = simpleFlamdexReaders[i];
                iterators.set(i, flamdexReader.getIntTermIterator(intField));
            }
        } catch (final Exception e) {
            close(iterators);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardIntTermIterator(iterators.toArray(new SimpleIntTermIterator[iterators.size()]));
    }

    MultiShardStringTermIterator stringTermOffsetIterator(final String stringField) throws  IOException {
        final List<SimpleStringTermIterator> iterators = Lists.newArrayListWithCapacity(simpleFlamdexReaders.length);
        try {
            for (final SimpleFlamdexReader flamdexReader : simpleFlamdexReaders) {
                iterators.add(flamdexReader.getStringTermIterator(stringField));
            }
        } catch (final Exception e) {
            close(iterators);
            throw Throwables.propagate(e);
        }
        return new SimpleMultiShardStringTermIterator(iterators.toArray(new SimpleStringTermIterator[iterators.size()]));
    }

    private void close(final List<? extends Closeable> closeables) {
        for (final Closeable closeable : closeables) {
            if (closeable != null) {
                Closeables2.closeQuietly(closeable, log);
            }
        }
    }
}
