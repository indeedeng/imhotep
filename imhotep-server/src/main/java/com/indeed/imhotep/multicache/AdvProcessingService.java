package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

/**
 * Created by darren on 3/16/15.
 */
public class AdvProcessingService<Data, Result> extends ProcessingService<Data, Result> {
    private final List<ProcessingQueuesHolder<Data, Result>> queuesList;
    private TaskCoordinator<Data> coordinator;

    public AdvProcessingService(TaskCoordinator<Data> coordinator) {
        super();
        this.coordinator = coordinator;
        this.queuesList = Lists.newArrayListWithCapacity(32);
    }

    @Override
    public int addTask(ProcessingTask<Data, Result> task) {
        final int handle = super.addTask(task);

        this.queuesList.add(new ProcessingQueuesHolder<Data, Result>());
        return handle;
    }

    @Override
    public void processData(Iterator<Data> iterator,
                            ResultProcessor<Result> resultProcessor) throws InterruptedException {
        if (resultProcessor != null) {
            this.resultProcessorThread = new Thread(resultProcessor);
            resultProcessor.setQueues(this.queuesList);
            this.resultProcessorThread.start();
        } else {
            for (ProcessingTask<Data, Result> task : this.tasks) {
                task.setDiscardResults(true);
            }
        }
        for (Thread t : this.threads) {
            t.start();
        }
        while (iterator.hasNext()) {
            final Data d = iterator.next();

            final int handle = this.coordinator.route(d);
            this.queuesList.get(handle).submitData(d);
        }
        this.awaitCompletion();
    }

    public static abstract class TaskCoordinator<Data> {
        public abstract int route(Data d);
    }
}
