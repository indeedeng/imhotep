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

import com.google.common.collect.Lists;
import com.indeed.flamdex.datastruct.CopyingBlockingQueue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by darren on 2/7/15.
 */
public class ProcessingService<Data, Result> {

    public ProcessingService(Data emptyDataObj, Result emptyResultObj) {
        this.emptyDataObj = emptyDataObj;
        this.emptyResultObj = emptyResultObj;
    }

    public static class ProcessingServiceException extends RuntimeException {
        public ProcessingServiceException(final Throwable wrapped) {
            super(wrapped);
        }
    }

    protected final ProcessingQueuesHolder queues = new ProcessingQueuesHolder();
    private final AtomicBoolean errorTracker = new AtomicBoolean(false);
    protected final ErrorCatcher errorCatcher = new ErrorCatcher();
    protected final List<ProcessingTask<Data, Result>> tasks = Lists.newArrayListWithCapacity(32);;
    protected final List<Thread> threads = Lists.newArrayListWithCapacity(32);
    protected Thread resultProcessorThread = null;
    protected int numTasks  = 0;
    protected final Data emptyDataObj;
    protected final Result emptyResultObj;

    public int addTask(final ProcessingTask<Data, Result> task) {
        tasks.add(task);
        task.configure(queues, Thread.currentThread(), emptyDataObj);

        final Thread thread = new Thread(task);
        thread.setUncaughtExceptionHandler(errorCatcher);
        threads.add(thread);

        final int taskNum = numTasks;
        ++numTasks;
        return taskNum;
    }

    public void processData(final Iterator<Data> iterator,
                            final ResultProcessor<Result> resultProcessor) {
        try {
            if (resultProcessor != null) {
                resultProcessorThread = new Thread(resultProcessor);
                final List<ProcessingQueuesHolder> singltonList = new ArrayList<>(1);
                singltonList.add(queues);
                resultProcessor.configure(singltonList,
                                          Thread.currentThread(),
                                          numTasks,
                                          emptyResultObj);
                resultProcessorThread.setUncaughtExceptionHandler(errorCatcher);
                threads.add(resultProcessorThread);
            }
            else {
                for (final ProcessingTask<Data, Result> task : tasks) {
                    task.setDiscardResults(true);
                }
            }

            for (final Thread thread : threads) thread.start();

            try {
                while (iterator.hasNext()) {
                    queues.submitData(iterator.next());
                }
                for (int count = 0; count < numTasks; ++count) {
                    queues.submitData(queues.COMPLETE_DATA_SENTINEL);
                }
            }
            catch (final InterruptedException ex) {
                for (final Thread thread: threads) thread.interrupt();
            }
            finally {
                join();
            }
        }
        catch (final Throwable throwable) {
            errorCatcher.uncaughtException(Thread.currentThread(), throwable);
        }
        finally {
            handleErrors();
        }
    }

    protected void handleErrors() {
        if (errorTracker.get()) {
            final List<Throwable> errors = errorCatcher.errors;
            final Throwable toThrow = errors.get(0);
            for (int i = 1; i < errors.size(); i++) {
                final Throwable toSuppress = errors.get(i);
                if (toThrow != toSuppress) {
                    toThrow.addSuppressed(toSuppress);
                }
            }
            throw new ProcessingServiceException(toThrow);
        }
    }

    protected void join() {
        while (!threads.isEmpty()) {
            try {
                threads.get(0).join();
                threads.remove(0);
            }
            catch (InterruptedException ex) {
                // !@# timeout and hurl up a runtime exception?
            }
        }
    }

    private static final Object COMPLETE_SENTINEL = new Object();

    public class ProcessingQueuesHolder {
        protected static final int QUEUE_SIZE = 8192;
        public final Data   COMPLETE_DATA_SENTINEL   = (Data)   COMPLETE_SENTINEL;
        public final Result COMPLETE_RESULT_SENTINEL = (Result) COMPLETE_SENTINEL;

        protected CopyingBlockingQueue<Data> dataQueue;
        protected CopyingBlockingQueue<Result> resultsQueue;

        public void submitData(final Data data) throws InterruptedException {
            dataQueue.put(data);
        }

        public void retrieveData(Data d) throws InterruptedException {
            dataQueue.take(d);
        }

        public void submitResult(final Result result) throws InterruptedException {
            resultsQueue.put(result);
        }

        public void retrieveResult(Result r) throws InterruptedException {
            resultsQueue.poll(10, TimeUnit.MICROSECONDS, r);
        }

        public void catchError(final Throwable throwable) {
            errorCatcher.uncaughtException(Thread.currentThread(), throwable);
        }
    }

    public class ErrorCatcher implements Thread.UncaughtExceptionHandler {
        private final List<Throwable> errors = new ArrayList<>();

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            errorTracker.set(true);
            errors.add(e);
        }
    }
}
