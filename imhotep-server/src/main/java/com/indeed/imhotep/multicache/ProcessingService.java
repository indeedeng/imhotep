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
import com.indeed.flamdex.datastruct.SingleProducerSingleConsumerBlockingQueue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Created by darren on 3/16/15.
 */
public class ProcessingService<Data, Result> {
    private final ErrorState errorState = new ErrorState();
    private final ErrorHandler errorHandler = new ErrorHandler();
    private final List<ProcessingTask<Data, Result>> tasks = Lists.newArrayListWithCapacity(32);
    private int numTasks = 0;
    private ResultProcessor<Result> resultProcessor;
    private CountDownLatch completionLatch;

    private final List<ProcessingQueuesHolder> queuesList;
    private final TaskCoordinator<Data> coordinator;
    private final ExecutorService threadPool;

    public ProcessingService(final TaskCoordinator<Data> coordinator,
                             final ExecutorService threadPool) {
        this.coordinator = coordinator;
        this.threadPool = threadPool;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    public int addTask(final ProcessingTask<Data, Result> task) {
        tasks.add(task);

        final ProcessingQueuesHolder queue = new ProcessingQueuesHolder();
        task.configure(queue, errorHandler);
        queuesList.add(queue);

        final int taskNum = numTasks;
        ++numTasks;
        return taskNum;
    }

    public void processData(final Iterator<Data> iterator,
                            final ResultProcessor<Result> resultProcessor) {
        if (resultProcessor != null) {
            this.resultProcessor = resultProcessor;
            this.completionLatch = new CountDownLatch(numTasks + 1);

            resultProcessor.configure(queuesList, errorHandler, numTasks);
        } else {
            this.completionLatch = new CountDownLatch(numTasks);

            for (final ProcessingTask<Data, Result> task : tasks) {
                task.setDiscardResults(true);
            }
        }

        /* generate data for the workers in a separate thread */
        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    while (iterator.hasNext()) {
                        final Data data = iterator.next();
                        final int handle = coordinator.route(data);
                        queuesList.get(handle).submitData(data);
                    }
                    for (final ProcessingQueuesHolder queue : queuesList) {
                        queue.submitData(queue.COMPLETE_DATA_SENTINEL);
                    }

                    completionLatch.await();
                    errorState.declareComplete();
                } catch (final InterruptedException ex) {
                    // thread is being stopped, just exit
                } catch (final Throwable throwable) {
                    errorHandler.declareError(throwable);
                }
            }
        });

        try {
            /* create the data processing threads after the producer */
            if (resultProcessor != null) {
                resultProcessor.setCompletionLatch(completionLatch);
                threadPool.execute(resultProcessor);
            }
            for (final ProcessingTask<Data, Result> task : tasks) {
                task.setCompletionLatch(completionLatch);
                threadPool.execute(task);
            }
        } catch (final RejectedExecutionException e) {
            // An error has probably already killed the executor service
        }

        try {
            errorState.await();
            if (errorState.isInError()) {
                /* wait 1 minute for all the threads to terminate */
                threadPool.awaitTermination(1, TimeUnit.MINUTES);
                handleErrors();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleErrors() {
        final List<Throwable> errors = errorHandler.errors;
        final Throwable toThrow = errors.get(0);
        for (int i = 1; i < errors.size(); i++) {
            final Throwable toSuppress = errors.get(i);
            if (toThrow != toSuppress) {
                toThrow.addSuppressed(toSuppress);
            }
        }
        throw new ProcessingServiceException(toThrow);
    }

    public class ProcessingQueuesHolder {
        private static final int QUEUE_SIZE = 8192;

        public final Data COMPLETE_DATA_SENTINEL;
        public final Result COMPLETE_RESULT_SENTINEL;

        private final BlockingQueue<Data> dataQueue;
        private final BlockingQueue<Result> resultsQueue;

        public ProcessingQueuesHolder() {
//            dataQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
//            resultsQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
            dataQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE);
            resultsQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE);

            this.COMPLETE_DATA_SENTINEL = (Data) new Object();
            this.COMPLETE_RESULT_SENTINEL = (Result) new Object();
        }

        public void submitData(final Data data) throws InterruptedException {
            dataQueue.put(data);
        }

        public Data retrieveData() throws InterruptedException {
            return dataQueue.take();
        }

        public void submitResult(final Result result) throws InterruptedException {
            resultsQueue.put(result);
        }

        public Result retrieveResult() throws InterruptedException {
            return resultsQueue.poll(10, TimeUnit.MICROSECONDS);
        }
    }

    public abstract static class TaskCoordinator<Data> {
        public abstract int route(Data data);
    }

    private static final class ErrorState {
        private boolean inError = false;

        synchronized void setError() {
            this.inError = true;
            notifyAll();
        }

        synchronized boolean isInError() {
            return this.inError;
        }

        synchronized void await() throws InterruptedException {
            if (this.inError)
                return;
            wait();
        }

        synchronized void declareComplete() {
            notifyAll();
        }
    }


    public final class ErrorHandler  {
        private final List<Throwable> errors = new ArrayList<>();

        public synchronized void declareError(Throwable e) {
            errors.add(e);
            errorState.setError();
            threadPool.shutdownNow();
        }
    }

    public static class ProcessingServiceException extends RuntimeException {
        public ProcessingServiceException(final Throwable wrapped) {
            super(wrapped);
        }
    }

}
