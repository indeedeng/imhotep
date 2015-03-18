package com.indeed.imhotep.multicache.ftgs;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author arun.
 */
class FTGSIterateRequestChannel {
    //TODO: use a non-locking single producer single consumer queue
    private final ArrayBlockingQueue<FTGSIterateRequest> ftgsIterateRequestQueue;
    private final ArrayBlockingQueue<FTGSIterateRequest> recycledFtgsIterateRequestQueue;
    private final FTGSIterateRequest ftgsIterateRequestBuff;
    private final int numShards;

    FTGSIterateRequestChannel(final int numShards, final int channelCapacity) {
        recycledFtgsIterateRequestQueue = new ArrayBlockingQueue<>(channelCapacity);
        ftgsIterateRequestQueue = new ArrayBlockingQueue<>(channelCapacity);
        ftgsIterateRequestBuff = new FTGSIterateRequest(numShards);
        this.numShards = numShards;
    }

    void submitRequest(final FTGSIterateRequest ftgsIterateRequest) {
        ftgsIterateRequestQueue.offer(ftgsIterateRequest);
    }

    FTGSIterateRequest getRecycledFtgsReq() {
        final FTGSIterateRequest ftgsRecycledReq = recycledFtgsIterateRequestQueue.poll();
        return ftgsRecycledReq == null ? new FTGSIterateRequest(numShards) : ftgsRecycledReq;
    }

    /**
     * Consumers of ftgs requests call this method. They should never store  the <code>FTGSIterateRequest</code> object
     * returned by this method. The object is reused and the same instance is returned for every call, except when
     * the request is <code>FTGSIterateRequest.END</code>
     */
    FTGSIterateRequest pollRequest() {
        {
            final FTGSIterateRequest ftgsIterateRequest = ftgsIterateRequestQueue.poll();
            //noinspection ObjectEquality
            if (ftgsIterateRequest == FTGSIterateRequest.END) {
                return ftgsIterateRequest;
            }
            copyToBuff(ftgsIterateRequest);
            recycledFtgsIterateRequestQueue.offer(ftgsIterateRequest);
        }
        return ftgsIterateRequestBuff;
    }

    private void copyToBuff(final FTGSIterateRequest ftgsIterateRequest) {
        ftgsIterateRequestBuff.isIntField = ftgsIterateRequest.isIntField;
        if (ftgsIterateRequest.isIntField) {
            ftgsIterateRequestBuff.intTerm = ftgsIterateRequest.intTerm;
        } else {
            ftgsIterateRequestBuff.setStringTerm(ftgsIterateRequest.stringTerm, ftgsIterateRequest.stringTermLength);
        }
        ftgsIterateRequestBuff.setOffsets(ftgsIterateRequest.offsets);
        ftgsIterateRequestBuff.outputSocket = ftgsIterateRequest.outputSocket;
        ftgsIterateRequestBuff.field = ftgsIterateRequest.field;
    }
}
