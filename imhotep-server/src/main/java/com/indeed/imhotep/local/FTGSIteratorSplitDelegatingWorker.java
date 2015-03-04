package com.indeed.imhotep.local;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
* @author arun.
*/
final class FTGSIteratorSplitDelegatingWorker implements Runnable {

    private final MultiShardFTGSExecutor delegate;

    FTGSIteratorSplitDelegatingWorker(MultiShardFTGSExecutor delegate, ConcurrentLinkedQueue<FTGSIterateRequest> ftgsIterateRequests) {
        this.delegate = delegate;
        //noinspection AssignmentToCollectionOrArrayFieldFromParameter
        this.ftgsIterateRequests = ftgsIterateRequests;
    }

    private final ConcurrentLinkedQueue<FTGSIterateRequest> ftgsIterateRequests;

    @Override
    public void run() {
        while (true) {
            final FTGSIterateRequest ftgsIterateRequest = ftgsIterateRequests.poll();
            //noinspection ObjectEquality
            if (ftgsIterateRequest == FTGSIterateRequest.END) {
                break;
            }
            if (ftgsIterateRequest.isIntField) {
                delegate.writeFTGSSplitForIntTerm(ftgsIterateRequest.field, ftgsIterateRequest.intTerm, ftgsIterateRequest.offsets, ftgsIterateRequest.outputSocket);
            } else {
                delegate.writeFTGSSplitForStringTerm(ftgsIterateRequest.field, ftgsIterateRequest.stringTerm, ftgsIterateRequest.stringTerm.length, ftgsIterateRequest.offsets, ftgsIterateRequest.outputSocket);
            }
        }
    }

}
