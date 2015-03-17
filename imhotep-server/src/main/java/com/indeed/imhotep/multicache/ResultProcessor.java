package com.indeed.imhotep.multicache;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ResultProcessor<Result> implements Runnable {
    protected ProcessingService.ProcessingQueuesHolder queue;

    public ProcessingService.ProcessingQueuesHolder getQueue() {
        return queue;
    }

    public void setQueue(ProcessingService.ProcessingQueuesHolder queue) {
        this.queue = queue;
    }

    public final void run() {
        Result r;

        try {
            init();
            do {
                try {
                    r = (Result) queue.retrieveResult();
                    if (r == ProcessingService.ProcessingQueuesHolder.TERMINATOR) {
                        break;
                    }
                    processResult(r);
                } catch (Throwable e) {
                    handleError(e);
                }
            } while (true);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        } finally {
            cleanup();
        }
    }

    protected void init() {

    }

    protected abstract void processResult(Result result);

    protected void handleError(Throwable e) throws Throwable {
        throw e;
    }

    protected void cleanup() {

    }

}

