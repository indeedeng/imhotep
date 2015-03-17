package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by darren on 2/7/15.
 */
public class ProcessingService<Data,Result> {
    protected final ProcessingQueuesHolder queues;
    protected final List<ProcessingTask<Data, Result>> tasks;
    protected int numTasks;
    protected List<Thread> threads;
    protected Thread resultProcessorThread;

    public ProcessingService() {
        this.queues = new ProcessingQueuesHolder();
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
                            ProcessingTask<Data, Result> resultProcessor) throws InterruptedException {
        if (resultProcessor != null) {
            this.resultProcessorThread = new Thread(resultProcessor);
            resultProcessor.setQueue(this.queues);
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
            this.submit(iterator.next());
        }
        this.awaitCompletion();
    }

    public void submit(Data d) throws InterruptedException {
        this.queues.submitData(d);
    }

    public Result retrieveResult() throws InterruptedException {
        return (Result) queues.retrieveResult();
    }

    public synchronized void awaitCompletion() throws InterruptedException {
        this.queues.waitUntilQueueIsEmpty();
    }

    static class ProcessingQueuesHolder {
        public static final Object TERMINATOR = new Object();

        private AtomicInteger numDataElementsQueued = new AtomicInteger(0);
        private volatile boolean waiting = false;

        protected BlockingQueue dataQueue;
        protected BlockingQueue resultsQueue;

        public ProcessingQueuesHolder() {
            this.dataQueue = new ArrayBlockingQueue(64);
            this.resultsQueue = new ArrayBlockingQueue(64);
        }

        public void submitData(Object d) throws InterruptedException {
            numDataElementsQueued.incrementAndGet();
            dataQueue.put(d);
        }

        public Object retrieveData() throws InterruptedException {
            Object d;
            int count;

            d = dataQueue.take();
            count = numDataElementsQueued.decrementAndGet();
            if (count == 0) {
                signalQueueEmpty();
            }
            return dataQueue.take();
        }

        public void submitResult(Object r) throws InterruptedException {
            resultsQueue.put(r);
        }

        public Object retrieveResult() throws InterruptedException {
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



