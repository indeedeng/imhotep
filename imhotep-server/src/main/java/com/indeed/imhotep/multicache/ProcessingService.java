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
import com.indeed.flamdex.datastruct.SingleProducerSingleConsumerBlockingQueue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by darren on 3/16/15.
 */
public class ProcessingService<Data, Result> {
    public static class ProcessingServiceException extends RuntimeException {
        public ProcessingServiceException(final Throwable wrapped) {
            super(wrapped);
        }
    }

    private final AtomicBoolean errorTracker = new AtomicBoolean(false);
    protected final ErrorCatcher errorCatcher = new ErrorCatcher();
    protected final List<ProcessingTask<Data, Result>> tasks = Lists.newArrayListWithCapacity(32);
    protected final List<Thread> threads = Lists.newArrayListWithCapacity(32);
    protected Thread resultProcessorThread = null;
    protected int numTasks = 0;

    private final List<ProcessingQueuesHolder> queuesList;
    private final TaskCoordinator<Data> coordinator;
    private CopyingBlockingQueue.ObjFactory<Data> dataFactory;
    private CopyingBlockingQueue.ObjCopier<Data> dataCopier;
    private Data dataSentinel;
    private CopyingBlockingQueue.ObjFactory<Result> resultFactory;
    private CopyingBlockingQueue.ObjCopier<Result> resultCopier;
    private Result resultSentinel;

    public ProcessingService(final TaskCoordinator<Data> coordinator,
                             final CopyingBlockingQueue.ObjFactory<Data> dataFactory,
                             final CopyingBlockingQueue.ObjCopier<Data> dataCopier,
                             final Data dataSentinel,
                             final CopyingBlockingQueue.ObjFactory<Result> resultFactory,
                             final CopyingBlockingQueue.ObjCopier<Result> resultCopier,
                             final Result resultSentinel) {
        this.coordinator = coordinator;
        this.dataFactory = dataFactory;
        this.dataCopier = dataCopier;
        this.dataSentinel = dataSentinel;
        this.resultFactory = resultFactory;
        this.resultCopier = resultCopier;
        this.resultSentinel = resultSentinel;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    public int addTask(final ProcessingTask<Data, Result> task) {
        tasks.add(task);

        final ProcessingQueuesHolder queue = new ProcessingQueuesHolder(dataFactory,
                                                                        dataCopier,
                                                                        dataSentinel,
                                                                        resultFactory,
                                                                        resultCopier,
                                                                        resultSentinel);
        task.configure(queue, Thread.currentThread(), dataFactory.newObj());
        queuesList.add(queue);

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
                resultProcessor.configure(queuesList,
                                          Thread.currentThread(),
                                          numTasks,
                                          resultFactory.newObj());
                resultProcessorThread.setUncaughtExceptionHandler(errorCatcher);
                threads.add(resultProcessorThread);
            } else {
                for (final ProcessingTask<Data, Result> task : tasks) {
                    task.setDiscardResults(true);
                }
            }

            for (final Thread thread : threads)
                thread.start();

            try {
                while (iterator.hasNext()) {
                    final Data data = iterator.next();
                    final int handle = coordinator.route(data);
                    queuesList.get(handle).submitData(data);
                }
                for (final ProcessingQueuesHolder queue : queuesList) {
                    queue.submitData(queue.COMPLETE_DATA_SENTINEL);
                }
            } catch (final InterruptedException ex) {
                for (final Thread thread : threads)
                    thread.interrupt();
            } finally {
                join();
            }
        } catch (final Throwable throwable) {
            errorCatcher.uncaughtException(Thread.currentThread(), throwable);
        } finally {
            handleErrors();
        }
    }

    public abstract static class TaskCoordinator<Data> {
        public abstract int route(Data data);
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
            } catch (InterruptedException ex) {
                // !@# timeout and hurl up a runtime exception?
            }
        }
    }

    public class ProcessingQueuesHolder {
        protected static final int QUEUE_SIZE = 8192;

        public final Data COMPLETE_DATA_SENTINEL;
        public final Result COMPLETE_RESULT_SENTINEL;

        protected final CopyingBlockingQueue<Data> dataQueue;
        protected final CopyingBlockingQueue<Result> resultsQueue;

        public ProcessingQueuesHolder(final CopyingBlockingQueue.ObjFactory<Data> dataFactory,
                                      final CopyingBlockingQueue.ObjCopier<Data> dataCopier,
                                      final Data dataSentinel,
                                      final CopyingBlockingQueue.ObjFactory<Result> resultFactory,
                                      final CopyingBlockingQueue.ObjCopier<Result> resultCopier,
                                      final Result resultSentinel) {
            dataQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE,
                                                                        dataFactory,
                                                                        dataCopier);
            resultsQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE,
                                                                           resultFactory,
                                                                           resultCopier);

            this.COMPLETE_DATA_SENTINEL = dataSentinel;
            this.COMPLETE_RESULT_SENTINEL = resultSentinel;
        }

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
