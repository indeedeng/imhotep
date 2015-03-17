package com.indeed.imhotep.multicache;

/**
 * Created by indeedLoan03 on 2/8/15.
 */
public abstract class ProcessingTask<Data,Result> implements Runnable {
    protected ProcessingService.ProcessingQueuesHolder queue;
    private boolean discardResults = false;

    public ProcessingService.ProcessingQueuesHolder getQueue() {
        return queue;
    }

    public void setQueue(ProcessingService.ProcessingQueuesHolder queue) {
        this.queue = queue;
    }

    public void setDiscardResults(boolean discardResults) {
        this.discardResults = discardResults;
    }

    public final void run() {
        Data d;
        Result r = null;
        int count;

        init();
        do {
            try {
                d = (Data)queue.retrieveData();
                if (d == ProcessingService.ProcessingQueuesHolder.TERMINATOR) {
                    break;
                }
                r = processData(d);
                if (! discardResults) {
                    queue.submitResult(r);
                }
            } catch (Throwable e) {
                handleError(e);
            }
        } while (true);
        cleanup();
    }

    protected abstract void init();

    protected abstract Result processData(Data data);

    protected abstract void handleError(Throwable e);

    protected abstract void cleanup();

}

