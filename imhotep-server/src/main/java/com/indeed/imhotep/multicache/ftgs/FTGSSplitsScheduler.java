package com.indeed.imhotep.multicache.ftgs;

import com.google.common.collect.Lists;
import com.indeed.flamdex.simple.MultiShardFlamdexReader;
import com.indeed.flamdex.simple.MultiShardIntTermIterator;
import com.indeed.flamdex.simple.MultiShardStringTermIterator;
import com.indeed.flamdex.simple.MultiShardTermIterator;
import com.indeed.imhotep.local.MTImhotepLocalMultiSession;
import com.indeed.imhotep.multicache.AdvProcessingService;
import com.indeed.util.core.hash.MurmurHash;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

/**
* Created by darren on 3/17/15.
*/
public final class FTGSSplitsScheduler extends AdvProcessingService<TermGroupStatOpDesc, Void>.TaskCoordinator {
    private static final int NUM_WORKERS = 8;
    private static final int CHANNEL_CAPACITY = 1000;
    public static final int LARGE_PRIME_FOR_CLUSTER_SPLIT = 969168349;

    private MTImhotepLocalMultiSession mtImhotepLocalMultiSession;
    private final MultiShardFlamdexReader multiShardFlamdexReader;
    private final String[] intFields;
    private final String[] stringFields;
    private final int numSplits;

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

        final List<MultiShardIntTermIterator> intTermIterators;
        intTermIterators = Lists.newArrayListWithCapacity(intFields.length);
        final List<MultiShardStringTermIterator> stringTermIterators;
        stringTermIterators = Lists.newArrayListWithCapacity(stringFields.length);

        for (final String intField : intFields) {
            intTermIterators.add(multiShardFlamdexReader.intTermOffsetIterator(intField));
        }

        for (final String stringField : stringFields) {
            stringTermIterators.add(multiShardFlamdexReader.stringTermOffsetIterator(stringField));
        }



        while (offsetIterator.next()) {
                    final long term = offsetIterator.term();
                    final long[] offsets = new long[numShards];
                    offsetIterator.offsets(offsets);
                    final FTGSIterateRequestChannel ftgsIterateRequestChannel = ftgsIterateRequestChannels.get(workerId);
                    final FTGSIterateRequest ftgsIterateRequest = ftgsIterateRequestChannel.getRecycledFtgsReq();
                    ftgsIterateRequest.setField(intField);
                    ftgsIterateRequest.setIntField(true);
                    ftgsIterateRequest.setIntTerm(term);
                    ftgsIterateRequest.setOffsets(offsets);
                    ftgsIterateRequest.setOutputSocket(mtImhotepLocalMultiSession.ftgsOutputSockets[splitIndex]);
                    ftgsIterateRequestChannel.submitRequest(ftgsIterateRequest);
                }

        for (final String stringField : stringFields) {
            try (final MultiShardStringTermIterator offsetIterator = multiShardFlamdexReader.stringTermOffsetIterator(stringField)) {
                while (offsetIterator.next()) {
                    final byte[] bytes = offsetIterator.termBytes();
                    final long[] offsets = new long[mtImhotepLocalMultiSession.sessions.length];
                    offsetIterator.offsets(offsets);
                    final FTGSIterateRequestChannel ftgsIterateRequestChannel = ftgsIterateRequestChannels.get(workerId);
                    final FTGSIterateRequest ftgsIterateRequest = ftgsIterateRequestChannel.getRecycledFtgsReq();
                    ftgsIterateRequest.setField(stringField);
                    ftgsIterateRequest.setIntField(false);
                    ftgsIterateRequest.setStringTerm(bytes, offsetIterator.termBytesLength());
                    ftgsIterateRequest.setOffsets(offsets);
                    ftgsIterateRequest.setOutputSocket(mtImhotepLocalMultiSession.ftgsOutputSockets[splitIndex]);
                    ftgsIterateRequestChannel.submitRequest(ftgsIterateRequest);
                }
            }
        }
//        for (int i = 0; i < NUM_WORKERS; i++) {
//            ftgsIterateRequestChannels.get(i).submitRequest(FTGSIterateRequest.END);
//        }
        return null;
    }

    public Iterator<TermGroupStatOpDesc> iterator() {
        return new Iterator<TermGroupStatOpDesc>() {
            private boolean hasNext
            private boolean iterateIntFields = true;
            private int current = 0;
            private MultiShardStringTermIterator currentStringTermIter;
            private MultiShardIntTermIterator currentIntTermIter;

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public TermGroupStatOpDesc next() {
                final TermGroupStatOpDesc desc;

                desc = new TermGroupStatOpDesc(numShards);
                while (true) {
                    if (iterateIntFields) {
                        if (!currentIntTermIter.next()) {
                            current++;
                            if (current >= intFields.length) {
                                iterateIntFields = false;
                                current = 0;
                                continue;
                            }
                            currentIntTermIter = multiShardFlamdexReader.intTermOffsetIterator(intFields[current]);
                            continue;
                        }
                        populateIntTermDesc(desc, currentIntTermIter);
                        return desc;
                    } else {
                        if (!currentStringTermIter.next()) {
                            current++;
                            if (current >= intFields.length) {
                                iterateIntFields = false;
                                continue;
                            }
                            currentStringTermIter = multiShardFlamdexReader.stringTermOffsetIterator(stringFields[current]);
                            continue;
                        }
                        populateStringTermDesc(desc, currentStringTermIter);
                        return desc;
                    }
                }


                return null;
            }

            private void populateIntTermDesc(final TermGroupStatOpDesc desc,
                                                final MultiShardIntTermIterator iter) {
                desc.isIntTerm = true;
                desc.intTerm = iter.term();
                desc.stringTerm = null;
                desc.size = iter.offsets(desc.offsets);
                iter.shardIds(desc.shardIds);
                iter.docCounts(desc.numDocsInTerm);
            }

            private void populateStringTermDesc(final TermGroupStatOpDesc desc,
                                                final MultiShardStringTermIterator iter) {
                desc.isIntTerm = false;
                desc.stringTerm = iter.termBytes();
                desc.stringTermLen = iter.termBytesLength();
                desc.size = iter.offsets(desc.offsets);
                iter.shardIds(desc.shardIds);
                iter.docCounts(desc.numDocsInTerm);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private static final int minHashInt(int term, int numSplits) {
        int v;

        v = term * LARGE_PRIME_FOR_CLUSTER_SPLIT;
        v += 12345 & Integer.MAX_VALUE;
        v = v >> 16;
        return v % numSplits;
    }

    private int minHashString(final byte[] termStringBytes, final int termStringLength, final int numSplits) {
        int v;

        v = MurmurHash.hash32(termStringBytes, 0, termStringLength);
        v *= LARGE_PRIME_FOR_CLUSTER_SPLIT;
        v += 12345 & 0x7FFFFFFF;
        v = v >> 16;
        return v % numSplits;
    }

    @Override
    public int route(TermGroupStatOpDesc desc) {
        if (desc.isIntTerm) {
            final int splitIndex = minHashInt(desc.intTerm, numSplits);
            return splitIndex % NUM_WORKERS;
        } else {
            final int splitIndex = minHashString(desc.stringTerm, desc.stringTermLen, numSplits);
            return splitIndex % NUM_WORKERS;
        }
    }
}
