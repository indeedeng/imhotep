package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.ArrayList;
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
    protected final ErrorTracker errorTracker;
    protected final ErrorCatcher errorCatcher;
    protected final List<ProcessingTask<Data, Result>> tasks;
    protected int numTasks;
    protected List<Thread> threads;
    protected Thread resultProcessorThread;

    public ProcessingService() {
        this.queues = new ProcessingQueuesHolder<>();
        this.errorTracker = new ErrorTracker();
        this.errorCatcher = new ErrorCatcher();
        this.tasks = Lists.newArrayListWithCapacity(32);
        this.threads = Lists.newArrayListWithCapacity(32);
        this.numTasks = 0;
    }

    public int addTask(ProcessingTask<Data, Result> task) {
        final int taskNum;
        final Thread t;

        this.tasks.add(task);
        task.setQueue(this.queues);
        task.setErrorTracker(this.errorTracker);

        t = new Thread(task);
        t.setUncaughtExceptionHandler(this.errorCatcher);
        this.threads.add(t);

        taskNum = this.numTasks;
        this.numTasks ++;
        return taskNum;
    }

    public void processData(Iterator<Data> iterator,
                            ResultProcessor<Result> resultProcessor) {
        try {
            if (resultProcessor != null) {
                this.resultProcessorThread = new Thread(resultProcessor);
                List<ProcessingQueuesHolder<?, Result>> singltonList = new ArrayList<>(1);
                singltonList.add(this.queues);
                resultProcessor.setQueues(singltonList);
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
                if (this.errorTracker.inError()) {
                    return;
                }
                this.queues.submitData(iterator.next());
            }
            this.queues.terminateQueue();
            this.awaitCompletion();
        } catch (Throwable t) {
            this.errorTracker.setError();
            this.errorCatcher.uncaughtException(Thread.currentThread(), t);
        } finally {
            handleErrors();
        }
    }

    protected void handleErrors() {
        if (this.errorTracker.inError()) {
            List<Throwable> errors = this.errorCatcher.errors;
            Throwable toThrow = errors.get(0);
            for (int i = 1; i < errors.size(); i++) {
                toThrow.addSuppressed(errors.get(i));
            }
            if (toThrow instanceof RuntimeException)
                throw (RuntimeException)toThrow;
            throw new RuntimeException(toThrow);
        }
    }

    public synchronized void awaitCompletion() throws InterruptedException {
        this.queues.waitUntilQueueIsEmpty();
    }

    public static class ProcessingQueuesHolder<D,R> {
        private static final Object TERMINATOR = new Object();

        private AtomicInteger numDataElementsQueued = new AtomicInteger(0);
        private volatile boolean waiting = false;

        protected BlockingQueue<Object> dataQueue;
        protected BlockingQueue<Object> resultsQueue;

        public ProcessingQueuesHolder() {
            this.dataQueue = new ArrayBlockingQueue<>(64);
            this.resultsQueue = new ArrayBlockingQueue<>(64);
        }

        public void submitData(D d) throws InterruptedException {
            numDataElementsQueued.incrementAndGet();
            dataQueue.put(d);
        }

        public D retrieveData() throws InterruptedException {
            final Object data;
            final int count;

            data = dataQueue.take();
            count = numDataElementsQueued.decrementAndGet();
            if (count == 0) {
                signalQueueEmpty();
            }
            if (data == TERMINATOR) {
                return null;
            }
            return (D)data;
        }

        public void submitResult(R r) throws InterruptedException {
            resultsQueue.put(r);
        }

        public R retrieveResult() throws InterruptedException {
            return (R)resultsQueue.take();
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

        public void terminateQueue() {
            this.dataQueue.add(TERMINATOR);
        }
    }

    public class ErrorCatcher implements Thread.UncaughtExceptionHandler {
        private final List<Throwable> errors = new ArrayList<>();

        @Override
        public synchronized void uncaughtException(Thread t, Throwable e) {
            this.errors.add(e);
            errorTracker.setError();
        }
    }

    public static class ErrorTracker {
        private boolean errorPreset = false;

        public synchronized void setError() {
            this.errorPreset = true;
        }

        public boolean inError() {
            return this.errorPreset;
        }
    }
}



