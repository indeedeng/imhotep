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


import java.util.concurrent.CountDownLatch;

/**
 * Created by darren on 2/8/15.
 */
public abstract class ProcessingTask<Data, Result> implements Runnable {
    private ProcessingService.ErrorHandler errorHandler;
    private CountDownLatch completionLatch;
    private boolean discardResults = false;

    protected ProcessingService<Data, Result>.ProcessingQueuesHolder queue;


    public void configure(final ProcessingService<Data, Result>.ProcessingQueuesHolder queue,
                          final ProcessingService.ErrorHandler errorHandler) {
        this.queue = queue;
        this.errorHandler = errorHandler;
    }

    public void setDiscardResults(final boolean discardResults) {
        this.discardResults = discardResults;
    }

    public void setCompletionLatch(CountDownLatch completionLatch) {
        this.completionLatch = completionLatch;
    }

    private void normalLoop() throws InterruptedException {
        Data data = queue.retrieveData();
        while (data != queue.COMPLETE_DATA_SENTINEL) {
                final Result result = processData(data);
                queue.submitResult(result);
                data = queue.retrieveData();
        }
        queue.submitResult(queue.COMPLETE_RESULT_SENTINEL);
    }

    private void discardLoop() throws InterruptedException {
        Data data = queue.retrieveData();
        while (data != queue.COMPLETE_DATA_SENTINEL) {
                processData(data);
                data = queue.retrieveData();
        }
    }

    public final void run() {
        try {

            init();
            if (!discardResults) {
                normalLoop();
            } else {
                discardLoop();
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

    protected abstract Result processData(Data data);

    protected void cleanup() { }
}

