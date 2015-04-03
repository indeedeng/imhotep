package com.indeed.imhotep.multicache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ResultProcessor<Result> implements Runnable {
    protected List<ProcessingService<?, Result>.ProcessingQueuesHolder> queues;

    private Thread owner = null;
    private int numTasks = 0;

    public void configure(List<? extends ProcessingService<?, Result>.ProcessingQueuesHolder> queues,
                          Thread owner,
                          int numTasks) {
        this.queues   = new ArrayList<>(queues);
        this.owner    = owner;
        this.numTasks = numTasks;
    }

    public final void run() {
        try {
            init();
            try {
                while (!queues.isEmpty() && numTasks > 0) {
                    final Iterator<ProcessingService<?, Result>.ProcessingQueuesHolder> iter = queues.iterator();
                    while (iter.hasNext()) {
                        final ProcessingService<?, Result>.ProcessingQueuesHolder queue = iter.next();
                        final Result result = queue.retrieveResult();
                        if (result == queue.COMPLETE_RESULT_SENTINEL) {
                            --numTasks;
                        }
                        else if (result != null) {
                            processResult(result);
                        }
                    }
                }
            }
            finally {
                cleanup();
            }
        }
        catch (InterruptedException ex) {
            // we've been signaled to go away
        }
        catch (Throwable throwable) {
            for (ProcessingService<?, Result>.ProcessingQueuesHolder queue: queues) { // !@#
                queue.catchError(throwable);
            }
            owner.interrupt();
        }
    }

    protected void init() { }
    protected abstract void processResult(Result result);
    protected void cleanup() { }
}

