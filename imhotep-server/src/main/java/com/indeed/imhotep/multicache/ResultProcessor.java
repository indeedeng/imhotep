package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ResultProcessor<Result> implements Runnable {
    protected List<ProcessingService.ProcessingQueuesHolder<?,Result>> queues;

    public void setQueues(List<ProcessingService.ProcessingQueuesHolder<?, Result>> queues) {
        this.queues = new ArrayList<>(queues);
    }

    public final void run() {
        Result r;

        try {
            init();
            outer: do {
                try {
                    final Iterator<ProcessingService.ProcessingQueuesHolder<?, Result>> iter;
                    iter = this.queues.iterator();
                    while (iter.hasNext()) {
                        final ProcessingService.ProcessingQueuesHolder<?, Result> q;
                        q = iter.next();
                        r = q.retrieveResult();
                        if (r == ProcessingService.ProcessingQueuesHolder.TERMINATOR) {
                            iter.remove();
                            continue;
                        }
                        processResult(r);
                        continue outer;
                    }
                    // should only get here is all the queues are
                    break;
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

