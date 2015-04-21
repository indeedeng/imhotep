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

import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 3/16/15.
 */
public class AdvProcessingService<Data, Result> extends ProcessingService<Data, Result> {
    private final List<ProcessingQueuesHolder> queuesList;
    private final TaskCoordinator<Data> coordinator;
    private CopyingBlockingQueue.ObjFactory<Data> dataFactory;
    private CopyingBlockingQueue.ObjCopier<Data> dataCopier;
    private CopyingBlockingQueue.ObjFactory<Result> resultFactory;
    private CopyingBlockingQueue.ObjCopier<Result> resultCopier;

    public AdvProcessingService(final TaskCoordinator<Data> coordinator,
                                final CopyingBlockingQueue.ObjFactory<Data> dataFactory,
                                final CopyingBlockingQueue.ObjCopier<Data> dataCopier,
                                final CopyingBlockingQueue.ObjFactory<Result> resultFactory,
                                final CopyingBlockingQueue.ObjCopier<Result> resultCopier) {
        super(dataFactory.newObj(), resultFactory.newObj());
        this.coordinator = coordinator;
        this.dataFactory = dataFactory;
        this.dataCopier = dataCopier;
        this.resultFactory = resultFactory;
        this.resultCopier = resultCopier;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    @Override
    public int addTask(final ProcessingTask<Data, Result> task) {
        final int handle = super.addTask(task);
        final ProcessingQueuesHolder queue =
                new AdvProcessingQueuesHolder(dataFactory, dataCopier, resultFactory, resultCopier);
        task.configure(queue, Thread.currentThread(), dataFactory.newObj());
        queuesList.add(queue);
        return handle;
    }

    @Override
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

    public class AdvProcessingQueuesHolder extends ProcessingQueuesHolder {

        public AdvProcessingQueuesHolder(CopyingBlockingQueue.ObjFactory<Data> dataFactory,
                                         CopyingBlockingQueue.ObjCopier<Data> dataCopier,
                                         CopyingBlockingQueue.ObjFactory<Result> resultFactory,
                                         CopyingBlockingQueue.ObjCopier<Result> resultCopier) {
            dataQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE,
                                                                        dataFactory,
                                                                        dataCopier);
            resultsQueue = new SingleProducerSingleConsumerBlockingQueue<>(QUEUE_SIZE,
                                                                           resultFactory,
                                                                           resultCopier);
        }
    }
}
