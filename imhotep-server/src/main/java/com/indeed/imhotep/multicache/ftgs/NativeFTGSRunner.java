package com.indeed.imhotep.multicache.ftgs;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.MultiShardFlamdexReader;
import com.indeed.imhotep.local.MultiCache;
import com.indeed.imhotep.multicache.ProcessingService;
import com.indeed.util.core.hash.MurmurHash;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by darren on 3/18/15.
 */
public class NativeFTGSRunner {
    private static final Logger log = Logger.getLogger(NativeFTGSRunner.class);
    private static final int NUM_WORKERS = 8;
    private static final int LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;

    private final MultiShardFlamdexReader multiShardFlamdexReader;
    private final long[] multicacheAddrs;
    private final int numGroups;
    private final int numMetrics;
    private final int numShards;

    static class NativeTGSinfo {
        public TermDesc termDesc;
        public int splitIndex;
        public int socketNum;
    }

    public NativeFTGSRunner(FlamdexReader[] flamdexReaders,
                            MultiCache[] multiCaches,
                            int numGroups,
                            int numMetrics) {
        this.numGroups = numGroups;
        this.numMetrics = numMetrics;
        this.multiShardFlamdexReader = new MultiShardFlamdexReader(flamdexReaders);
        this.numShards = flamdexReaders.length;
        this.multicacheAddrs = new long[multiCaches.length];
        for (int i = 0; i < multiCaches.length; i++) {
            this.multicacheAddrs[i] = multiCaches[i].getNativeAddress();
        }
    }

    private static final int minHashInt(long term, int numSplits) {
        int v;

        v = (int) (term * LARGE_PRIME_FOR_CLUSTER_SPLIT);
        v += 12345;
        v &= Integer.MAX_VALUE;
        v = v >> 16;
        return v % numSplits;
    }

    private static final int minHashString(final byte[] termStringBytes,
                                           final int termStringLength,
                                           final int numSplits) {
        int v;

        v = MurmurHash.hash32(termStringBytes, 0, termStringLength);
        v *= LARGE_PRIME_FOR_CLUSTER_SPLIT;
        v += 12345;
        v &= 0x7FFFFFFF;
        v = v >> 16;
        return v % numSplits;
    }

    public void run(final String[] intFields,
                    final String[] stringFields,
                    final int numSplits,
                    final Socket[] sockets) throws InterruptedException, IOException {

        final ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setDaemon(true);
        builder.setNameFormat("Native-FTGS-Thread -%d");
        final ExecutorService threadPool;
        threadPool = Executors.newFixedThreadPool(NUM_WORKERS + 2, builder.build());

        for (String field : intFields) {
            final CloseableIter iter = createIntIterator(field, numSplits);
            try {
                internalRun(field, true, iter, sockets, numSplits, threadPool);
            } finally {
                iter.close();
            }
        }
        for (String field : stringFields) {
            final CloseableIter iter = createStringIterator(field, numSplits);
            try {
                internalRun(field, false, iter, sockets, numSplits, threadPool);
            } finally {
                iter.close();
            }
        }
        
        /* Write "no more fields" terminator to the sockets */
        for (int i = 0; i < numSplits; i++) {
            final Socket s = sockets[i];
            final OutputStream out = s.getOutputStream();
            out.write(0);
        }
    }

    private void internalRun(String field,
                             boolean isIntField,
                             final Iterator<NativeTGSinfo> iter,
                             final Socket[] sockets,
                             final int nSockets,
                             ExecutorService threadPool) {
        final ProcessingService<NativeTGSinfo, Void> service;
        final ProcessingService.TaskCoordinator<NativeTGSinfo> router;
        router = new ProcessingService.TaskCoordinator<NativeTGSinfo>() {
            @Override
            public int route(NativeTGSinfo info) {
                return info.splitIndex % NUM_WORKERS;
            }
        };
        service = new ProcessingService<>(router, threadPool);
        for (int i = 0; i < NUM_WORKERS; i++) {
            final List<Socket> socketList = new ArrayList<>();
            for (int j = 0; j < nSockets; j++) {
                if (j % NUM_WORKERS == i) {
                    socketList.add(sockets[j]);
                }
            }
            final Socket[] mySockets = socketList.toArray(new Socket[socketList.size()]);
            service.addTask(new NativeFTGSWorker(field,
                                                 isIntField,
                                                 numGroups,
                                                 numMetrics,
                                                 numShards,
                                                 mySockets,
                                                 i,
                                                 multicacheAddrs));
        }

        service.processData(iter, null);
    }

    private CloseableIter createIntIterator(final String intField, final int numSplits) {
        try {
            final Iterator<TermDesc> iter;
            iter = multiShardFlamdexReader.intTermOffsetIterator(intField);
            final Iterator<NativeTGSinfo> modifiedIter;
            modifiedIter = Iterators.transform(iter, new Function<TermDesc, NativeTGSinfo>() {
                @Nullable
                @Override
                public NativeTGSinfo apply(@Nullable TermDesc desc) {
                    final NativeTGSinfo info = new NativeTGSinfo();
                    info.termDesc = desc;
                    info.splitIndex = minHashInt(desc.intTerm, numSplits);
                    info.socketNum = info.splitIndex / NUM_WORKERS;
                    return info;
                }
            });

            return new CloseableIter(iter, modifiedIter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CloseableIter createStringIterator(final String stringField, final int numSplits) {
        try {
            final Iterator<TermDesc> iter;
            iter = multiShardFlamdexReader.stringTermOffsetIterator(stringField);
            final Iterator<NativeTGSinfo> modifiedIter;
            modifiedIter = Iterators.transform(iter, new Function<TermDesc, NativeTGSinfo>() {
                @Nullable
                @Override
                public NativeTGSinfo apply(@Nullable TermDesc desc) {
                    final NativeTGSinfo info = new NativeTGSinfo();
                    info.termDesc = desc;
                    info.splitIndex = minHashString(desc.stringTerm, desc.stringTermLen, numSplits);
                    info.socketNum = info.splitIndex / NUM_WORKERS;
                    return info;
                }
            });

            return new CloseableIter(iter, modifiedIter);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final class CloseableIter implements Closeable, Iterator<NativeTGSinfo> {
        private Iterator<TermDesc> closeable;
        private final Iterator<NativeTGSinfo> iter;

        private CloseableIter(Iterator<TermDesc> closeable, Iterator<NativeTGSinfo> iter) {
            this.closeable = closeable;
            this.iter = iter;
        }

        @Override
        public void close() throws IOException {
            ((Closeable)closeable).close();
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public NativeTGSinfo next() {
            return iter.next();
        }

        @Override
        public void remove() {
            iter.remove();
        }
    }
}
