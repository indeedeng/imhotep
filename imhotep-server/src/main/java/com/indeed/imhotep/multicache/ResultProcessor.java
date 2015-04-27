/*
 * Copyright (C) 2015 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.indeed.imhotep.multicache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ResultProcessor<Result> implements Runnable {
    protected List<ProcessingService<?, Result>.ProcessingQueuesHolder> queues;

    private int numTasks = 0;
    private CountDownLatch completionLatch;
    private ProcessingService.ErrorHandler errorHandler;

    public void configure(final List<? extends ProcessingService<?, Result>.ProcessingQueuesHolder> queues,
                          final ProcessingService.ErrorHandler errorHandler,
                          final int numTasks) {
        this.errorHandler = errorHandler;
        this.queues   = new ArrayList<>(queues);
        this.numTasks = numTasks;
    }

    public void setCompletionLatch(CountDownLatch completionLatch) {
        this.completionLatch = completionLatch;
    }

    public final void run() {
        try {
            init();

            while (!queues.isEmpty() && numTasks > 0) {
                final Iterator<ProcessingService<?, Result>.ProcessingQueuesHolder> iter;
                iter = queues.iterator();

                while (iter.hasNext()) {
                    final ProcessingService<?, Result>.ProcessingQueuesHolder queue = iter.next();
                    Result result = queue.retrieveResult();
                    if (result == queue.COMPLETE_RESULT_SENTINEL) {
                        --numTasks;
                    } else if (result != null) {
                        processResult(result);
                    }
                }
            }

            complete();
        } catch (final InterruptedException ex) {
            // we've been signaled to go away
        } catch (final Throwable throwable) {
            errorHandler.declareError(throwable);
        } finally {
            cleanup();
            completionLatch.countDown();
        }
    }

    protected void init() { }
    protected void complete() { }
    protected abstract void processResult(Result result);
    protected void cleanup() { }
}

