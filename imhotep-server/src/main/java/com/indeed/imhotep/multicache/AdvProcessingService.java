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

import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 3/16/15.
 */
public class AdvProcessingService<Data, Result> extends ProcessingService<Data, Result> {
    private final List<ProcessingQueuesHolder> queuesList;
    private final TaskCoordinator<Data> coordinator;

    public AdvProcessingService(final TaskCoordinator<Data> coordinator) {
        super();
        this.coordinator = coordinator;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    @Override
    public int addTask(final ProcessingTask<Data, Result> task) {
        final int handle = super.addTask(task);
        final ProcessingQueuesHolder queue = new ProcessingQueuesHolder();
        task.configure(queue, Thread.currentThread());
        queuesList.add(queue);
        return handle;
    }

    @Override
    public void processData(final Iterator<Data> iterator,
                            final ResultProcessor<Result> resultProcessor) {
        try {
            if (resultProcessor != null) {
                resultProcessorThread = new Thread(resultProcessor);
                resultProcessor.configure(queuesList, Thread.currentThread(), numTasks);
                resultProcessorThread.setUncaughtExceptionHandler(errorCatcher);
                threads.add(resultProcessorThread);
            }
            else {
                for (final ProcessingTask<Data, Result> task : tasks) {
                    task.setDiscardResults(true);
                }
            }

            for (final Thread thread : threads)  thread.start();

            try {
                while (iterator.hasNext()) {
                    final Data data   = iterator.next();
                    final int  handle = coordinator.route(data);
                    queuesList.get(handle).submitData(data);
                }
                for (final ProcessingQueuesHolder queue: queuesList) {
                    queue.submitData(queue.COMPLETE_DATA_SENTINEL);
                }
            }
            catch (final InterruptedException ex) {
                for (final Thread thread: threads) thread.interrupt();
            }
            finally {
                for (final Thread thread: threads) thread.join();
            }
        }
        catch (final Throwable throwable) {
            errorTracker.set(true);
            errorCatcher.uncaughtException(Thread.currentThread(), throwable);
        }
        finally {
            handleErrors();
        }
    }

    public abstract static class TaskCoordinator<Data> {
        public abstract int route(Data data);
    }
}
