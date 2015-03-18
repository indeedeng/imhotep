package com.indeed.imhotep.multicache;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ProcessingTask<Data,Result> implements Runnable {
    protected ProcessingService.ProcessingQueuesHolder<Data,Result> queue;
    private boolean discardResults = false;

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
            init();
            do {
                try {
                    d = queue.retrieveData();
                    if (d == ProcessingService.ProcessingQueuesHolder.TERMINATOR) {
                        break;
                    }
                    r = processData(d);
                    if (!discardResults) {
                        queue.submitResult(r);
                    }
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

    protected abstract Result processData(Data data);

    protected void handleError(Throwable e) throws Throwable {
        throw e;
    }

    protected void cleanup() {

    }

}

