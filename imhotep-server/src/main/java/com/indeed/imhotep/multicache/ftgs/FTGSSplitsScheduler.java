package com.indeed.imhotep.multicache.ftgs;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.indeed.flamdex.simple.MultiShardFlamdexReader;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.multicache.AdvProcessingService;
import com.indeed.util.core.hash.MurmurHash;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

/**
* Created by darren on 3/17/15.
*/
public final class FTGSSplitsScheduler extends AdvProcessingService.TaskCoordinator<TermDesc> {
    private static final int NUM_WORKERS = 8;
    private static final int CHANNEL_CAPACITY = 1000;
    public static final int LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;

    private MTImhotepLocalMultiSession mtImhotepLocalMultiSession;
    private final MultiShardFlamdexReader multiShardFlamdexReader;
    private final String[] intFields;
    private final String[] stringFields;
    private final int numSplits;

    private static class NativeTGSinfo {
        public TermDesc termDesc;
        public int splitIndex;
        public Socket socket;
    }

    private FTGSSplitsScheduler(MTImhotepLocalMultiSession mtImhotepLocalMultiSession,
                                MultiShardFlamdexReader multiShardFlamdexReader,
                                String[] intFields,
                                String[] stringFields,
                                int numSplits) {
        this.mtImhotepLocalMultiSession = mtImhotepLocalMultiSession;
        this.multiShardFlamdexReader = multiShardFlamdexReader;
        this.intFields = intFields;
        this.stringFields = stringFields;
        this.numSplits = numSplits;
    }

    @Override
    public Void call() throws Exception {
//        final CountDownLatch latch = writeFTGSSplitReqLatch.get();
//        latch.await();
//        writeFTGSSplitReqLatch.set(null);
//        final List<FTGSIterateRequestChannel> ftgsIterateRequestChannels = Lists.newArrayListWithCapacity(
//                NUM_WORKERS);
//        for (int i = 0; i < NUM_WORKERS; i++) {
//            ftgsIterateRequestChannels.set(i, new FTGSIterateRequestChannel(
//                    mtImhotepLocalMultiSession.sessions.length, CHANNEL_CAPACITY));
//            //TODO: supply an actual implementation
//            final FTGSIteratorSplitDelegatingWorker worker = new FTGSIteratorSplitDelegatingWorker(null, ftgsIterateRequestChannels.get(i));
//            mtImhotepLocalMultiSession.ftgsExecutorService.submit(worker);
//        }


        final List<Closeable> iterators;
        iterators = Lists.newArrayListWithCapacity(intFields.length + stringFields.length);

        final Function<String, Iterator<TermDesc>> intIterBuilder;
        final Function<String, Iterator<TermDesc>> stringIterBuilder;
        intIterBuilder = new Function<String, Iterator<TermDesc>>() {
            @Nullable
            @Override
            public Iterator<TermDesc> apply(String intField) {
                final Iterator<TermDesc> iter;
                iter = multiShardFlamdexReader.intTermOffsetIterator(intField);
                iterators.add((Closeable)iter);
                return iter;
            }
        };
        stringIterBuilder = new Function<String, Iterator<TermDesc>>() {
            @Nullable
            @Override
            public Iterator<TermDesc> apply(String stringField) {
                final Iterator<TermDesc> iter;
                iter = multiShardFlamdexReader.stringTermOffsetIterator(stringField);
                iterators.add((Closeable)iter);
                return iter;
            }
        };

        final Iterator<Iterator<TermDesc>> intIterIter;
        final Iterator<Iterator<TermDesc>> stringIterIter;
        intIterIter = Iterators.transform(Iterators.forArray(intFields), intIterBuilder);
        stringIterIter = Iterators.transform(Iterators.forArray(stringFields), stringIterBuilder);

        final Iterator<Iterator<TermDesc>> allTermsIterIter;
        allTermsIterIter = Iterators.concat(intIterIter, stringIterIter);

        final Iterator<TermDesc> allTermsIter = Iterators.concat(allTermsIterIter);
        Iterators.transform(allTermsIter, new Function<TermDesc, NativeTGSinfo>() {
            private final int minHashInt(long term, int numSplits) {
                int v;

                v = (int) (term * LARGE_PRIME_FOR_CLUSTER_SPLIT);
                v += 12345 & Integer.MAX_VALUE;
                v = v >> 16;
                return v % numSplits;
            }

            private int minHashString(final byte[] termStringBytes,
                                             final int termStringLength,
                                             final int numSplits) {
                int v;

                v = MurmurHash.hash32(termStringBytes, 0, termStringLength);
                v *= LARGE_PRIME_FOR_CLUSTER_SPLIT;
                v += 12345 & 0x7FFFFFFF;
                v = v >> 16;
                return v % numSplits;
            }

            @Nullable
            @Override
            public NativeTGSinfo apply(@Nullable TermDesc desc) {
                NativeTGSinfo info = new NativeTGSinfo();
                info.termDesc = desc;
                if (desc.isIntTerm) {
                    info.splitIndex = minHashInt(desc.intTerm, numSplits);
                } else {
                    info.splitIndex = minHashString(desc.stringTerm, desc.stringTermLen, numSplits);
                }
                info.socket = sockets[info.splitIndex];
                return info;
            }
        });

        while (offsetIterator.next()) {
            offsetIterator.offsets(offsets);
            final FTGSIterateRequestChannel ftgsIterateRequestChannel = ftgsIterateRequestChannels
                    .get(workerId);
            final FTGSIterateRequest ftgsIterateRequest = ftgsIterateRequestChannel
                    .getRecycledFtgsReq();
            ftgsIterateRequest
                    .setOutputSocket(mtImhotepLocalMultiSession.ftgsOutputSockets[splitIndex]);
        }

//        for (int i = 0; i < NUM_WORKERS; i++) {
//            ftgsIterateRequestChannels.get(i).submitRequest(FTGSIterateRequest.END);
//        }
        return null;
    }


}
