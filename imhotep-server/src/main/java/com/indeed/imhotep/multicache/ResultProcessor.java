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

/**
 * Created by darren on 2/8/15.
 */
public abstract class ResultProcessor<Result> implements Runnable {
    protected List<ProcessingService<?, Result>.ProcessingQueuesHolder> queues;

    private Thread owner = null;
    private int numTasks = 0;

    public void configure(final List<? extends ProcessingService<?, Result>.ProcessingQueuesHolder> queues,
                          final Thread owner,
                          final int numTasks) {
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
        catch (final InterruptedException ex) {
            // we've been signaled to go away
        }
        catch (final Throwable throwable) {
            for (final ProcessingService<?, Result>.ProcessingQueuesHolder queue: queues) {
                queue.catchError(throwable);
            }
            owner.interrupt();
        }
    }

    protected void init() { }
    protected abstract void processResult(Result result);
    protected void cleanup() { }
}

