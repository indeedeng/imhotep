package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by darren on 2/7/15.
 */
public class ProcessingService<Data,Result> {
    protected final ProcessingQueuesHolder<Data, Result> queues;
    protected final List<ProcessingTask<Data, Result>> tasks;
    protected int numTasks;
    protected List<Thread> threads;
    protected Thread resultProcessorThread;

    public ProcessingService() {
        this.queues = new ProcessingQueuesHolder<>();
        this.tasks = Lists.newArrayListWithCapacity(32);
        this.threads = Lists.newArrayListWithCapacity(32);
        this.numTasks = 0;
    }

    public int addTask(ProcessingTask<Data, Result> task) {
        final int taskNum;

        this.tasks.add(task);
        task.setQueue(this.queues);
        this.threads.add(new Thread(task));

        taskNum = this.numTasks;
        this.numTasks ++;
        return taskNum;
    }

    public void processData(Iterator<Data> iterator,
                            ResultProcessor<Result> resultProcessor) throws InterruptedException {
        if (resultProcessor != null) {
            this.resultProcessorThread = new Thread(resultProcessor);
            List<ProcessingQueuesHolder<?, Result>> singltonList = new ArrayList<>(1);
            singltonList.add(this.queues);
            resultProcessor.setQueues(singltonList);
            this.resultProcessorThread.start();
        } else {
            for (ProcessingTask<Data,Result> task : this.tasks) {
                task.setDiscardResults(true);
            }
        }
        for (Thread t : this.threads) {
            t.start();
        }
        while (iterator.hasNext()) {
            this.queues.submitData(iterator.next());
        }
        this.awaitCompletion();
    }

    public synchronized void awaitCompletion() throws InterruptedException {
        this.queues.waitUntilQueueIsEmpty();
    }

    public static class ProcessingQueuesHolder<D,R> {
        public static final Object TERMINATOR = new Object();

        private AtomicInteger numDataElementsQueued = new AtomicInteger(0);
        private volatile boolean waiting = false;

        protected BlockingQueue<D> dataQueue;
        protected BlockingQueue<R> resultsQueue;

        public ProcessingQueuesHolder() {
            this.dataQueue = new ArrayBlockingQueue<D>(64);
            this.resultsQueue = new ArrayBlockingQueue<R>(64);
        }

        public void submitData(D d) throws InterruptedException {
            numDataElementsQueued.incrementAndGet();
            dataQueue.put(d);
        }

        public D retrieveData() throws InterruptedException {
            D d;
            int count;

            d = dataQueue.take();
            count = numDataElementsQueued.decrementAndGet();
            if (count == 0) {
                signalQueueEmpty();
            }
            return d;
        }

        public void submitResult(R r) throws InterruptedException {
            resultsQueue.put(r);
        }

        public R retrieveResult() throws InterruptedException {
            return resultsQueue.take();
        }

        public synchronized void waitUntilQueueIsEmpty() throws InterruptedException {
            while (numDataElementsQueued.get() > 0) {
                waiting = true;
                this.wait();
            }
        }

        private synchronized void signalQueueEmpty() {
            if (waiting && numDataElementsQueued.get() == 0) {
                waiting = false;
                this.notifyAll();
            }
        }
    }
}



