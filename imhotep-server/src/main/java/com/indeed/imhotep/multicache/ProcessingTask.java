package com.indeed.imhotep.multicache;

import com.indeed.imhotep.multicache.ProcessingService.AdjustableLatch;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ProcessingTask<Data,Result> implements Runnable {
    protected ProcessingService.ProcessingQueuesHolder<Data,Result> queue;
    protected AdjustableLatch finishedLatch;
    private boolean discardResults = false;
    private ProcessingService.ErrorTracker errorTracker;

    public ProcessingService.ProcessingQueuesHolder<Data, Result> getQueue() {
        return queue;
    }

    public void setQueue(ProcessingService.ProcessingQueuesHolder<Data, Result> queue) {
        this.queue = queue;
    }

    public void setDiscardResults(boolean discardResults) {
        this.discardResults = discardResults;
    }

    public final void run() {
        Data d;
        Result r;

        try {
            try {
                init();
                do {
                    try {
                        if (errorTracker.inError()) {
                            /* some other thread has thrown an error */
                            return;
                        }
                        d = queue.retrieveData();
                        if (d == null) {
                            break;
                        }
                        r = processData(d);
                        if (!discardResults) {
                            queue.submitResult(r);
                        }
                    } catch (Exception e) {
                        handleError(e);
                    }
                } while (true);
            } finally {
                cleanup();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            this.finishedLatch.countDown();
        }
    }

    protected void init() {

    }

    protected abstract Result processData(Data data);

    protected void handleError(Exception e) {
        throw new RuntimeException(e);
    }

    protected void cleanup() {

    }

    public void setErrorTracker(ProcessingService.ErrorTracker errorTracker) {
        this.errorTracker = errorTracker;
    }

    public void setLatch(AdjustableLatch latch) {
        this.finishedLatch = latch;
    }
}

