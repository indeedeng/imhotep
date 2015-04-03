package com.indeed.imhotep.multicache;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ProcessingTask<Data, Result> implements Runnable {
    protected ProcessingService<Data, Result>.ProcessingQueuesHolder queue;

    private Thread  owner          = null;
    private boolean discardResults = false;

    public void configure(ProcessingService<Data, Result>.ProcessingQueuesHolder queue, Thread owner) {
        this.queue = queue;
        this.owner = owner;
    }

    public void setDiscardResults(boolean discardResults) {
        this.discardResults = discardResults;
    }

    private final void normalLoop() throws InterruptedException {
        boolean alive = true;
        Data data = queue.retrieveData();
        while (data != queue.COMPLETE_DATA_SENTINEL && alive) {
            try {
                final Result result = processData(data);
                queue.submitResult(result);
                data = queue.retrieveData();
            }
            catch (InterruptedException ex) {
                alive = false;
            }
            catch (Throwable thr) {
                alive = false;
                queue.catchError(thr);
                owner.interrupt();
            }
        }
        if (alive) queue.submitResult(queue.COMPLETE_RESULT_SENTINEL);
    }

    private final void discardLoop() throws InterruptedException {
        boolean alive = true;
        Data data = queue.retrieveData();
        while (data != queue.COMPLETE_DATA_SENTINEL && alive) {
            try {
                processData(data);
                data = queue.retrieveData();
            }
            catch (InterruptedException ex) {
                alive = false;
            }
            catch (Throwable thr) {
                alive = false;
                queue.catchError(thr);
                owner.interrupt();
            }
        }
    }

    public final void run() {
        try {
            init();
            try {
                if (!discardResults)
                    normalLoop();
                else
                    discardLoop();
            }
            finally {
                cleanup();
            }
        }
        catch (InterruptedException ex) {
            // we've been signaled to go away
        }
        catch (Throwable throwable) {
            queue.catchError(throwable);
            owner.interrupt();
        }
    }

    protected void init() { }

    protected abstract Result processData(Data data);

    protected void cleanup() { }
}

