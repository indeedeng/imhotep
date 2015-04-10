package com.indeed.flamdex.datastruct;

import junit.framework.Assert;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author arun.
 */
public class TestSingleProducerSingleConsumerBlockingQueue {

    public TestSingleProducerSingleConsumerBlockingQueue(){}

    private static final Logger log = Logger.getLogger(TestSingleProducerSingleConsumerBlockingQueue.class);
    private static final Payload TERMINATOR = new Payload(0, 0);

    private static final class Payload {
        final int seq;
        final int work;

        Payload(int seq, int work) {
            this.seq = seq;
            this.work = work;
        }

        @Override
        public String toString() {
            return String.valueOf(seq);
        }
    }

    private static final class Producer implements Callable<Integer> {
        private final BlockingQueue<Payload> queue;
        private final Random random = new Random();

        Producer(BlockingQueue<Payload> queue) {
            //noinspection AssignmentToCollectionOrArrayFieldFromParameter
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            int numAdded = 0;
            int numPuts = 0;
            int numOffers = 0;
            int numOfferTimeouts = 0;
            for (int i = 0; i < 1000000; i++) {
                final Payload payload = new Payload(i, random.nextInt(50));
                switch (random.nextInt(3)) {
                    case 0:
                        queue.put(payload);
                        numPuts++;
                        break;
                    case 1:
                        //noinspection StatementWithEmptyBody
                        while(!queue.offer(payload));
                        numOffers++;
                        break;
                    case 2:
                        //noinspection StatementWithEmptyBody
                        while(!queue.offer(payload, 50, TimeUnit.MILLISECONDS));
                        numOfferTimeouts++;
                }
                numAdded++;
            }
            queue.put(TERMINATOR);
            log.info("numPuts = " + numPuts);
            log.info("numOffers = " + numOffers);
            log.info("numOfferTimeouts = " + numOfferTimeouts);
            return numAdded;
        }
    }

    private static final class Consumer implements Callable<Integer> {
        private final BlockingQueue<Payload> queue;
        private final Random random = new Random();

        Consumer(BlockingQueue<Payload> queue) {
            //noinspection AssignmentToCollectionOrArrayFieldFromParameter
            this.queue = queue;
        }

        @Override
        public Integer call() throws Exception {
            int previousSeqNumber = -1;
            int numConsumed = 0;
            int numTakes = 0;
            int numPolls = 0;
            int numPollTimeouts = 0;
            long sum = 0;
            while (true) {
                Payload payload;
                switch (random.nextInt(3)) {
                    case 0:
                        payload = queue.take();
                        numTakes++;
                        break;
                    case 1:
                        //noinspection NestedAssignment,StatementWithEmptyBody
                        while((payload = queue.poll()) == null);
                        numPolls++;
                        break;
                    case 2:
                        //noinspection NestedAssignment,StatementWithEmptyBody
                        while((payload = queue.poll(50, TimeUnit.MILLISECONDS)) == null);
                        numPollTimeouts++;
                        break;
                    default:
                        throw new RuntimeException("unreachable!");
                }
                //noinspection ObjectEquality
                if (payload == TERMINATOR) {
                    break;
                }
                Assert.assertTrue(payload.seq == previousSeqNumber + 1);
                previousSeqNumber = payload.seq;
                for (int i = 0; i < payload.work; i++) {
                    sum += i;
                }
                numConsumed++;
            }
            log.info("sum = " + sum);
            log.info("numTakes = " + numTakes);
            log.info("numPolls = " + numPolls);
            log.info("numPollTimeouts = " + numPollTimeouts);
            return numConsumed;
        }
    }


    private static final ExecutorService executorService = Executors.newFixedThreadPool(12);

    @Test
    public void testSPSCBQ() throws InterruptedException, ExecutionException {
        BasicConfigurator.configure();
        final BlockingQueue<Payload> queue = new SingleProducerSingleConsumerBlockingQueue<>(4096);
        final Producer producer = new Producer(queue);
        final Consumer consumer = new Consumer(queue);
        final long blockQueue = System.currentTimeMillis();
        final Future<Integer> consumerFuture = executorService.submit(consumer);
        final Future<Integer> producerFuture = executorService.submit(producer);
        final int numAdded = producerFuture.get();
        final int numConsumed = consumerFuture.get();
        log.info(System.currentTimeMillis() - blockQueue);
        log.info("numConsumed = " + numConsumed);
        log.info("numAdded = " + numAdded);
        executorService.shutdown();
    }
}
