/*
 * Copyright (C) 2014 Indeed Inc.
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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.*;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.junit.*;
import org.junit.rules.*;

/**
 * @author jfinley
 */
public class TestProcessingService {

    class TaskException      extends RuntimeException { }
    class ProcessorException extends RuntimeException { }

    Random            random;
    AtomicInteger     processorCleanupCounter;
    AtomicInteger     taskCleanupCounter;
    IntGenerator      generator;
    ExecutorService   executorService;

    @Rule
    public Timeout globalTimeout = new Timeout(100000);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        random                  = new Random(0x42);
        processorCleanupCounter = new AtomicInteger(0);
        taskCleanupCounter      = new AtomicInteger(0);
        generator               = new IntGenerator(1000000);
        executorService         = createExecutorService();
    }

    @After
    public void tearDown() {
        Assert.assertEquals("processor cleanup", 0, processorCleanupCounter.get());
        Assert.assertEquals("task cleanup",      0, taskCleanupCounter.get());
        executorService.shutdown();
    }

    ProcessingService.TaskCoordinator<Long> newRouter(final int numTasks) {
        return new ProcessingService.TaskCoordinator<Long>() {
            @Override
                public int route(Long data) { return (int) Math.abs(data) % numTasks; }
        };
    }

    private ExecutorService createExecutorService() {
        final ThreadFactoryBuilder builder = new ThreadFactoryBuilder();
        builder.setDaemon(true);
        builder.setNameFormat("Native-FTGS-Thread -%d");
        final ExecutorService threadPool;
        threadPool = Executors.newCachedThreadPool(builder.build());

        return threadPool;
    }

    void advancedTask(final int numTasks, Processor processor) {
        ProcessingService service =
                new ProcessingService<Long, Long>(newRouter(numTasks), executorService);
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new Task());
        service.processData(generator, processor);
    }

    @Test public void advancedTask1() { advancedTask(1, new Processor()); }
    @Test public void advancedTask2() { advancedTask(2, new Processor()); }
    @Test public void advancedTask16() { advancedTask(16, new Processor()); }
    @Test public void advancedTask128() { advancedTask(128, new Processor()); }
    @Test public void advancedTaskDiscard1() { advancedTask(1, null); }
    @Test public void advancedTaskDiscard2() { advancedTask(2, null); }
    @Test public void advancedTaskDiscard16() { advancedTask(16, null); }
    @Test public void advancedTaskDiscard128() { advancedTask(128, null); }

    void advancedBrokenTask(final int numTasks, Processor processor) {
        ProcessingService service = new ProcessingService<Long, Long>(newRouter(numTasks),
                                                                      executorService);
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new BrokenTask());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, processor);
    }

    @Test public void advancedBrokenTask1() { advancedBrokenTask(1, new Processor()); }
    @Test public void advancedBrokenTask2() { advancedBrokenTask(2, new Processor()); }
    @Test public void advancedBrokenTask16() { advancedBrokenTask(16, new Processor()); }
    @Test public void advancedBrokenTask128() { advancedBrokenTask(128, new Processor()); }
    @Test public void advancedBrokenTaskDiscard1() { advancedBrokenTask(1, null); }
    @Test public void advancedBrokenTaskDiscard2() { advancedBrokenTask(2, null); }
    @Test public void advancedBrokenTaskDiscard16() { advancedBrokenTask(16, null); }
    @Test public void advancedBrokenTaskDiscard128() { advancedBrokenTask(128, null); }

    void advancedBrokenProcessor(final int numTasks) {
        ProcessingService service = new ProcessingService<Long, Long>(newRouter(numTasks),
                                                                      executorService);
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new Task());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, new BrokenProcessor());
    }

    @Test public void advancedBrokenProcessor1() { advancedBrokenProcessor(1); }
    @Test public void advancedBrokenProcessor2() { advancedBrokenProcessor(2); }
    @Test public void advancedBrokenProcessor16() { advancedBrokenProcessor(16); }
    @Test public void advancedBrokenProcessor128() { advancedBrokenProcessor(128); }

    void advancedBrokenTaskAndProcessor(final int numTasks) {
        ProcessingService service = new ProcessingService<Long, Long>(newRouter(numTasks),
                                                                      createExecutorService());
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new BrokenTask());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, new BrokenProcessor());
    }

    @Test public void advancedBrokenTaskAndProcessor1() { advancedBrokenTaskAndProcessor(1); }
    @Test public void advancedBrokenTaskAndProcessor2() { advancedBrokenTaskAndProcessor(2); }
    @Test public void advancedBrokenTaskAndProcessor16() { advancedBrokenTaskAndProcessor(16); }
    @Test public void advancedBrokenTaskAndProcessor128() { advancedBrokenTaskAndProcessor(128); }

    class IntGenerator implements Iterator<Long> {
        private long count;
        public IntGenerator(final long count) { this.count = count; }
        public boolean hasNext() { return count > 0; }
        public Long next() { --count; return new Long(random.nextInt()); }
        public void remove() { }
    }

    class Task extends ProcessingTask <Long, Long> {
        @Override
        public void init() { taskCleanupCounter.incrementAndGet(); }

        @Override
        public void cleanup() { taskCleanupCounter.decrementAndGet(); }

        @Override
        protected Long processData(final Long data) {
            long result = 0;
            for (long value = data % 1023; value > 1; --value) {
                result *= value;
            }
            return new Long(result);
        }
    }

    class BrokenTask extends Task {
        private int counter = 1000;

        @Override
        protected Long processData(final Long data) {
            --counter;
            if (counter <= 0) throw new TaskException();
            return super.processData(data);
        }
    }

    class Processor extends ResultProcessor<Long> {
        @Override
        public void init() { processorCleanupCounter.incrementAndGet(); }

        @Override
        public void cleanup() { processorCleanupCounter.decrementAndGet(); }

        @Override
        protected void processResult(Long data) {
            long result = 0;
            for (long value = data % 1023; value > 1; --value) {
                result *= value;
            }
        }
    }

    class BrokenProcessor extends Processor {
        private int counter = 1000;

        @Override
        protected void processResult(final Long data) {
            --counter;
            if (counter <= 0) throw new ProcessorException();
            super.processResult(data);
        }
    }
}