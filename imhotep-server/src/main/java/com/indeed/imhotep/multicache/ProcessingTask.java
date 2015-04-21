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


/**
 * Created by darren on 2/8/15.
 */
public abstract class ProcessingTask<Data, Result> implements Runnable {
    protected ProcessingService<Data, Result>.ProcessingQueuesHolder queue;

    private Thread  owner          = null;
    private boolean discardResults = false;
    private Data dataObj;

    public void configure(final ProcessingService<Data, Result>.ProcessingQueuesHolder queue,
                          final Thread owner,
                          final Data emptyDataObj) {
        this.queue = queue;
        this.owner = owner;
        this.dataObj = emptyDataObj;
    }

    public void setDiscardResults(final boolean discardResults) {
        this.discardResults = discardResults;
    }

    private void normalLoop() throws InterruptedException {
        boolean alive = true;
        queue.retrieveData(dataObj);
        while (dataObj != queue.COMPLETE_DATA_SENTINEL && alive) {
            try {
                final Result result = processData(dataObj);
                queue.submitResult(result);
                queue.retrieveData(dataObj);
            }
            catch (final InterruptedException ex) {
                alive = false;
            }
            catch (final Throwable thr) {
                alive = false;
                queue.catchError(thr);
                owner.interrupt();
            }
        }
        if (alive) queue.submitResult(queue.COMPLETE_RESULT_SENTINEL);
    }

    private void discardLoop() throws InterruptedException {
        boolean alive = true;
        queue.retrieveData(dataObj);
        while (dataObj != queue.COMPLETE_DATA_SENTINEL && alive) {
            try {
                processData(dataObj);
                queue.retrieveData(dataObj);
            }
            catch (final InterruptedException ex) {
                alive = false;
            }
            catch (final Throwable thr) {
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
        catch (final InterruptedException ex) {
            // we've been signaled to go away
        }
        catch (final Throwable throwable) {
            queue.catchError(throwable);
            owner.interrupt();
        }
    }

    protected void init() { }

    protected abstract Result processData(Data data);

    protected void cleanup() { }
}

