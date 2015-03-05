package com.indeed.imhotep.local;

/**
* @author arun.
*/
final class FTGSIteratorSplitDelegatingWorker implements Runnable {

    private final FTGSIterateRequestChannel ftgsReqChannel;
    private final MultiShardFTGSExecutor delegate;

    FTGSIteratorSplitDelegatingWorker(MultiShardFTGSExecutor delegate, FTGSIterateRequestChannel ftgsReqChannel) {
        this.delegate = delegate;
        this.ftgsReqChannel = ftgsReqChannel;
    }

    @Override
    public void run() {
        while (true) {
            final FTGSIterateRequest ftgsIterateRequest = ftgsReqChannel.pollRequest();
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
