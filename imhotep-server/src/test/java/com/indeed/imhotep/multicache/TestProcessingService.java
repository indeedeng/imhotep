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
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.junit.*;
import org.junit.rules.*;
import org.junit.Assert.*;

/**
 * @author jfinley
 */
public class TestProcessingService {

    class TaskException      extends RuntimeException { }
    class ProcessorException extends RuntimeException { }

    Random            random;
    AtomicInteger     cleanupCounter;
    IntGenerator      generator;
    ProcessingService service;

    @Rule
    public Timeout globalTimeout = new Timeout(10000);

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void setUp() {
        random         = new Random(0x42);
        cleanupCounter = new AtomicInteger(0);
        generator      = new IntGenerator(1000000);
        service        = new ProcessingService();
    }

    @After
    public void tearDown() {
        Assert.assertEquals(0, cleanupCounter.get());
    }

    void normalTask(final int numTasks, Processor processor) {
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new Task());
        service.processData(generator, processor);
    }

    @Test public void task1() { normalTask(1, new Processor()); }
    @Test public void task2() { normalTask(2, new Processor()); }
    @Test public void task16() { normalTask(16, new Processor()); }
    @Test public void task128() { normalTask(128, new Processor()); }
    @Test public void taskDiscard1() { normalTask(1, null); }
    @Test public void taskDiscard2() { normalTask(2, null); }
    @Test public void taskDiscard16() { normalTask(16, null); }
    @Test public void taskDiscard128() { normalTask(128, null); }

    void brokenTask(final int numTasks, Processor processor) {
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new BrokenTask());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, new Processor());
    }

    @Test public void brokenTask1() { brokenTask(1, new Processor()); }
    @Test public void brokenTask2() { brokenTask(2, new Processor()); }
    @Test public void brokenTask16() { brokenTask(16, new Processor()); }
    @Test public void brokenTask128() { brokenTask(128, new Processor()); }
    @Test public void brokenTaskDiscard1() { brokenTask(1, null); }
    @Test public void brokenTaskDiscard2() { brokenTask(2, null); }
    @Test public void brokenTaskDiscard16() { brokenTask(16, null); }
    @Test public void brokenTaskDiscard128() { brokenTask(128, null); }

    void brokenProcessor(final int numTasks) {
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new Task());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, new BrokenProcessor());
    }

    @Test public void brokenProcessor1() { brokenProcessor(1); }
    @Test public void brokenProcessor2() { brokenProcessor(2); }
    @Test public void brokenProcessor16() { brokenProcessor(16); }
    @Test public void brokenProcessor128() { brokenProcessor(128); }

    void brokenTaskAndProcessor(final int numTasks) {
        for (int count = 0; count < numTasks; ++count)
            service.addTask(new BrokenTask());
        exception.expect(ProcessingService.ProcessingServiceException.class);
        service.processData(generator, new BrokenProcessor());
    }

    @Test public void brokenTaskAndProcessor1() { brokenTaskAndProcessor(1); }
    @Test public void brokenTaskAndProcessor2() { brokenTaskAndProcessor(2); }
    @Test public void brokenTaskAndProcessor16() { brokenTaskAndProcessor(16); }
    @Test public void brokenTaskAndProcessor128() { brokenTaskAndProcessor(128); }

    AdvProcessingService.TaskCoordinator<Long> newRouter(final int numTasks) {
        return new AdvProcessingService.TaskCoordinator<Long>() {
            @Override
                public int route(Long data) { return Math.abs(data.intValue()) % numTasks; }
        };
    }

    public void advancedTask(final int numTasks, Processor processor) {
        service = new AdvProcessingService<Long, Long>(newRouter(numTasks));
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

    public void advancedBrokenTask(final int numTasks, Processor processor) {
        service = new AdvProcessingService<Long, Long>(newRouter(numTasks));
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
        service = new AdvProcessingService<Long, Long>(newRouter(numTasks));
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
        service = new AdvProcessingService<Long, Long>(newRouter(numTasks));
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
        public void init() { cleanupCounter.incrementAndGet(); }

        @Override
        public void cleanup() { cleanupCounter.decrementAndGet(); }

        @Override
        protected Long processData(final Long data) {
            long result = 0;
            for (long value = data % 1023; value > 1; --value) {
                result *= value;
            }
            return Long.valueOf(result);
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
        public void init() { cleanupCounter.incrementAndGet(); }

        @Override
        public void cleanup() { cleanupCounter.decrementAndGet(); }

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
