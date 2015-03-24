package com.indeed.imhotep.multicache.ftgs;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.simple.MultiShardFlamdexReader;
import com.indeed.imhotep.local.MultiCache;
import com.indeed.imhotep.multicache.AdvProcessingService;
import com.indeed.util.core.hash.MurmurHash;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 3/18/15.
 */
public class NativeFtgsRunner {
    private static final Logger log = Logger.getLogger(NativeFtgsRunner.class);
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

    public NativeFtgsRunner(FlamdexReader[] flamdexReaders,
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
        v += 12345 & Integer.MAX_VALUE;
        v = v >> 16;
        return v % numSplits;
    }

    private static final int minHashString(final byte[] termStringBytes,
                              final int termStringLength,
                              final int numSplits) {
        int v;

        v = MurmurHash.hash32(termStringBytes, 0, termStringLength);
        v *= LARGE_PRIME_FOR_CLUSTER_SPLIT;
        v += 12345 & 0x7FFFFFFF;
        v = v >> 16;
        return v % numSplits;
    }

    public void run(final String[] intFields,
                    final String[] stringFields,
                    final int numSplits,
                    final Socket[] sockets) throws InterruptedException, IOException {
        for (String field : intFields) {
            final Iterator<NativeTGSinfo> iter = createIntIterator(field, numSplits);
            try {
                internalRun(field, true, iter, sockets);
            } finally {
                ((Closeable)iter).close();
            }
        }
        for (String field : stringFields) {
            final Iterator<NativeTGSinfo> iter = createStringIterator(field, numSplits);
            try {
                internalRun(field, false, iter, sockets);
            } finally {
                ((Closeable)iter).close();
            }
        }
    }

    private void internalRun(String field,
                             boolean isIntField,
                             final Iterator<NativeTGSinfo> iter,
                             final Socket[] sockets) throws InterruptedException {
        final AdvProcessingService<NativeTGSinfo, Void> service;
        final AdvProcessingService.TaskCoordinator<NativeTGSinfo> router;
        router = new AdvProcessingService.TaskCoordinator<NativeTGSinfo>() {
            @Override
            public int route(NativeTGSinfo info) {
                return info.splitIndex % NUM_WORKERS;
            }
        };
        service = new AdvProcessingService<>(router);
        for (int i = 0; i < NUM_WORKERS; i++) {
            final List<Socket> socketList = new ArrayList<>();
            for (int j = 0; j < sockets.length; j++) {
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

    private Iterator<NativeTGSinfo> createIntIterator(final String intField, final int numSplits) {
        try {
            final Iterator<TermDesc> iter;
            iter = multiShardFlamdexReader.intTermOffsetIterator(intField);

            return Iterators.transform(iter, new Function<TermDesc, NativeTGSinfo>() {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Iterator<NativeTGSinfo> createStringIterator(final String stringField,
                                                         final int numSplits) {
        try {
            final Iterator<TermDesc> iter;
            iter = multiShardFlamdexReader.stringTermOffsetIterator(stringField);

            return Iterators.transform(iter, new Function<TermDesc, NativeTGSinfo>() {
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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
