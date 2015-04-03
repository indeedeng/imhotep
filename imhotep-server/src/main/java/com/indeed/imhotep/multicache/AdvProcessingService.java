package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 3/16/15.
 */
public class AdvProcessingService<Data, Result> extends ProcessingService<Data, Result> {
    private final List<ProcessingQueuesHolder> queuesList;
    private TaskCoordinator<Data> coordinator;

    public AdvProcessingService(TaskCoordinator<Data> coordinator) {
        super();
        this.coordinator = coordinator;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    @Override
    public int addTask(ProcessingTask<Data, Result> task) {
        final int handle = super.addTask(task);
        ProcessingQueuesHolder queue = new ProcessingQueuesHolder();
        task.configure(queue, Thread.currentThread());
        this.queuesList.add(queue);
        return handle;
    }

    @Override
    public void processData(Iterator<Data> iterator,
                            ResultProcessor<Result> resultProcessor) {
        try {
            if (resultProcessor != null) {
                this.resultProcessorThread = new Thread(resultProcessor);
                resultProcessor.configure(this.queuesList, Thread.currentThread(), numTasks);
                resultProcessorThread.setUncaughtExceptionHandler(errorCatcher);
                threads.add(resultProcessorThread);
            }
            else {
                for (ProcessingTask<Data, Result> task : this.tasks) {
                    task.setDiscardResults(true);
                }
            }

            for (Thread t : this.threads)  t.start();

            try {
                while (iterator.hasNext()) {
                    final Data data   = iterator.next();
                    final int  handle = coordinator.route(data);
                    queuesList.get(handle).submitData(data);
                }
                for (ProcessingQueuesHolder queue: queuesList) {
                    queue.submitData(queue.COMPLETE_DATA_SENTINEL);
                }
            }
            catch (InterruptedException ex) {
                for (Thread thread: threads) thread.interrupt();
            }
            finally {
                for (Thread thread: threads) thread.join();
            }
        }
        catch (Throwable t) {
            this.errorTracker.set(true);
            this.errorCatcher.uncaughtException(Thread.currentThread(), t);
        }
        finally {
            handleErrors();
        }
    }

    public static abstract class TaskCoordinator<Data> {
        public abstract int route(Data d);
    }
}
