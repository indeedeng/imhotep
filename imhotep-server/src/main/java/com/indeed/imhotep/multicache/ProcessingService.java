package com.indeed.imhotep.multicache;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by darren on 2/7/15.
 */
public class ProcessingService<Data,Result> {

    public static class ProcessingServiceException extends RuntimeException {
        public ProcessingServiceException(Throwable wrapped) {
            super(wrapped);
        }
    }

    protected final ProcessingQueuesHolder queues = new ProcessingQueuesHolder();
    protected final AtomicBoolean errorTracker = new AtomicBoolean(false);
    protected final ErrorCatcher errorCatcher = new ErrorCatcher();;
    protected final List<ProcessingTask<Data, Result>> tasks = Lists.newArrayListWithCapacity(32);;
    protected final List<Thread> threads = Lists.newArrayListWithCapacity(32);
    protected Thread resultProcessorThread = null;
    protected int numTasks  = 0;

    public int addTask(ProcessingTask<Data, Result> task) {
        final int taskNum;
        final Thread t;

        this.tasks.add(task);

        task.configure(queues, Thread.currentThread());

        t = new Thread(task);
        t.setUncaughtExceptionHandler(this.errorCatcher);
        this.threads.add(t);

        taskNum = this.numTasks;
        ++this.numTasks;
        return taskNum;
    }

    public void processData(Iterator<Data> iterator,
                            ResultProcessor<Result> resultProcessor) {
        try {
            if (resultProcessor != null) {
                resultProcessorThread = new Thread(resultProcessor);
                List<ProcessingQueuesHolder> singltonList = new ArrayList<>(1);
                singltonList.add(queues);
                resultProcessor.configure(singltonList, Thread.currentThread(), numTasks);
                resultProcessorThread.setUncaughtExceptionHandler(errorCatcher);
                threads.add(resultProcessorThread);
            } else {
                for (ProcessingTask<Data, Result> task : tasks) {
                    task.setDiscardResults(true);
                }
            }

            for (Thread thread : threads) thread.start();

            try {
                while (iterator.hasNext()) {
                    queues.submitData(iterator.next());
                }
                for (int count = 0; count < numTasks; ++count) {
                    queues.submitData(queues.COMPLETE_DATA_SENTINEL);
                }
            }
            catch (InterruptedException ex) {
                for (Thread thread: threads) thread.interrupt();
            }
            finally {
                for (Thread thread: threads) thread.join();
            }
        } catch (Throwable throwable) {
            errorCatcher.uncaughtException(Thread.currentThread(), throwable);
        } finally {
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

    private static final Object COMPLETE_SENTINEL = new Object();

    public class ProcessingQueuesHolder {
        public final Data   COMPLETE_DATA_SENTINEL   = (Data)   COMPLETE_SENTINEL;
        public final Result COMPLETE_RESULT_SENTINEL = (Result) COMPLETE_SENTINEL;

        protected BlockingQueue<Data>   dataQueue    = new ArrayBlockingQueue<Data>(64);
        protected BlockingQueue<Result> resultsQueue = new ArrayBlockingQueue<Result>(64);

        public void submitData(Data data) throws InterruptedException {
            dataQueue.put(data);
        }

        public Data retrieveData() throws InterruptedException {
            return dataQueue.take();
        }

        public void submitResult(Result result) throws InterruptedException {
            resultsQueue.put(result);
        }

        public Result retrieveResult() throws InterruptedException {
            return resultsQueue.poll(10, TimeUnit.MICROSECONDS);
        }

        public void catchError(Throwable throwable) {
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
