package com.indeed.imhotep.commands;

import com.google.common.base.Throwables;
import com.indeed.flamdex.MemoryFlamdex;
import com.indeed.imhotep.SlotTiming;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.io.RequestTools;
import com.indeed.imhotep.local.ImhotepJavaLocalSession;
import com.indeed.imhotep.local.ImhotepLocalSession;
import com.indeed.imhotep.scheduling.ImhotepTask;
import com.indeed.imhotep.scheduling.SchedulerType;
import com.indeed.imhotep.scheduling.SilentCloseable;
import com.indeed.imhotep.scheduling.TaskScheduler;
import com.indeed.imhotep.service.MetricStatsEmitter;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCommandYield {

    private final List<String> scheduleOrder = new ArrayList<>();

    private static final String USER1 = "userName1";
    private static final String CLIENT1 = "ClientName1";
    private static final String USER2 = "userName2";
    private static final String CLIENT2 = "ClientName2";
    private static final String COMMANDID1 = "commandId1";
    private static final String COMMANDID2 = "commandId2";
    private static final String SESSIONID = "randomSessionIdString";

    private static TaskScheduler oldCPUScheduler = TaskScheduler.CPUScheduler;

    private ImhotepCommand<Void> getSleepingCommand(final long milliSeconds, final String commandId) {
        return new VoidAbstractImhotepCommand(SESSIONID) {
            @Override
            public void applyVoid(final ImhotepSession imhotepSession) {
                synchronized (scheduleOrder) {
                    scheduleOrder.add(commandId);
                }
                // Instead of Thread.sleep(milliSeconds) hack start time.
                ImhotepTask.THREAD_LOCAL_TASK.get().changeTaskStartTime(milliSeconds);
            }

            @Override
            protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
                return null;
            }
        };
    }

    @Before
    public void setup() {
        TaskScheduler.CPUScheduler = new TaskScheduler(1, TimeUnit.SECONDS.toNanos(60) , TimeUnit.SECONDS.toNanos(1), 50, SchedulerType.CPU, MetricStatsEmitter.NULL_EMITTER);

        ImhotepTask.setup(USER1, CLIENT1, (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
            // Hacking user consumption stats, so USER1 have 1sec of execution already when each test starts.
            ImhotepTask.THREAD_LOCAL_TASK.get().changeTaskStartTime(1000);
        }

        ImhotepTask.setup(USER2, CLIENT2, (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
        }
        ImhotepTask.clear();
    }

    @After
    public void clear() {
        TaskScheduler.CPUScheduler = oldCPUScheduler;
        scheduleOrder.clear();
    }

    public Thread getCommandThread(final String username, final String clientName, final String commandID, final long... commandsExecTimeMillis) {
        return new Thread(() -> {
            try(final ImhotepLocalSession imhotepLocalSession = new ImhotepJavaLocalSession(SESSIONID, new MemoryFlamdex(), null )) {

                final List<ImhotepCommand> firstCommands = new ArrayList<>();
                for (int i = 0; i < (commandsExecTimeMillis.length - 1); i++) {
                    firstCommands.add(getSleepingCommand(commandsExecTimeMillis[i], commandID));
                }

                final ImhotepCommand<Void> lastCommand = getSleepingCommand(commandsExecTimeMillis[commandsExecTimeMillis.length - 1], commandID);

                ImhotepTask.setup(username, clientName, (byte)0, new SlotTiming());
                try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
                    imhotepLocalSession.executeBatchRequest(firstCommands, lastCommand);
                }
            } catch (final ImhotepOutOfMemoryException e) {
                Throwables.propagate(e);
            }
        });
    }

    private static void executeThreads(final Thread thread1, final Thread thread2) throws InterruptedException {
        ImhotepTask.setup("TestUsername", "testClient", (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
            thread1.start();
            thread2.start();
            Thread.sleep(100);
        }

        thread1.join();
        thread2.join();
        ImhotepTask.clear();
    }

    @Test
    public void testInterleaving() throws InterruptedException {
        final Thread thread1 = getCommandThread(USER1, CLIENT1, COMMANDID1, 0);
        final Thread thread2 = getCommandThread(USER2, CLIENT2, COMMANDID2, 2000, 2000);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID1, COMMANDID2));
    }

    @Test
    public void testUnweave() throws InterruptedException {
        final Thread thread1 = getCommandThread(USER1, CLIENT1, COMMANDID1, 0);
        final Thread thread2 = getCommandThread(USER2, CLIENT2, COMMANDID2, 200L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID1));
    }

    @Test
    public void testLongInterleave() throws InterruptedException {
        final Thread thread1 = getCommandThread(USER1, CLIENT1, COMMANDID1, 0);
        final Thread thread2 = getCommandThread(USER2, CLIENT2, COMMANDID2, 100L, 100L, 900L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID2));
    }

    @Test
    public void testLongerInterleave() throws InterruptedException {
        final Thread thread1 = getCommandThread(USER1, CLIENT1, COMMANDID1, 1000L, 200L);
        final Thread thread2 = getCommandThread(USER2, CLIENT2, COMMANDID2, 100L, 100L, 900L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID2, COMMANDID1));
    }

    @Test
    public void testLongerInterleave1() throws InterruptedException {
        final Thread thread1 = getCommandThread(USER1, CLIENT1, COMMANDID1, 100L, 100L);
        final Thread thread2 = getCommandThread(USER2, CLIENT2, COMMANDID2, 100L, 100L, 1900L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID1, COMMANDID2));
    }

    private static void runFullUtilizationTest(final int slotsCount, final int taskCount, final int taskTimeMillis) {
        // Goal of this test is to check that yield works and scheduler switches task in the middle of execution.
        // Let's say we have 3 task 1000 milliseconds each and we want to execute them on 2 execution slots.
        // Then without yielding total wallclock time will be 2000 millis (first 1000 millis 2 task are executed on 2 slots,
        // then third task takes another 1000 millis on one slot)
        // With yielding total wallclock time will be 1500 millis because the will be switching between tasks
        // and 3000 millis of execution time are divided between 2 slots

        Assert.assertTrue(slotsCount < taskCount);
        final Thread[] threads = new Thread[taskCount];
        for (int i = 0; i < taskCount; i++) {
            final int index = i; // constant copy to pass in lambda
            threads[i] = new Thread(() -> {
                ImhotepTask.setup("user" + index, "client" + index, (byte)0, new SlotTiming());
                try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
                    final int waitMillis = 10; // yielding every 10 millis
                    final int iterCount = taskTimeMillis / waitMillis;
                    for (int iter = 0; iter < iterCount; iter++) {
                        Thread.sleep(waitMillis);
                        TaskScheduler.CPUScheduler.yieldIfNecessary();
                    }
                } catch (final InterruptedException e) {
                    Throwables.propagate(e);
                }
            });
            }
        final TaskScheduler old = TaskScheduler.CPUScheduler;
        TaskScheduler.CPUScheduler = new TaskScheduler(slotsCount, TimeUnit.SECONDS.toNanos(60) , TimeUnit.SECONDS.toNanos(1),
                100 /*executionChunkMillis*/, SchedulerType.CPU, MetricStatsEmitter.NULL_EMITTER);

        final long start = System.currentTimeMillis();
        for (final Thread thread : threads) {
            thread.start();
        }
        for (final Thread thread : threads) {
            try {
                thread.join();
            } catch (final InterruptedException ignored) {
            }
        }
        final long end = System.currentTimeMillis();

        TaskScheduler.CPUScheduler = old;

        // Check that all slots were busy all the time.
        // Add extra 100 millis for scheduling + Thread.start + Thread.join overhead.
        Assert.assertTrue( end - start < taskCount * taskTimeMillis / slotsCount + 500);
    }

    @Test
    public void testYield() {
        runFullUtilizationTest(2, 3, 1000);
        runFullUtilizationTest(5, 7, 1500);
        runFullUtilizationTest(10, 11, 1000);
        runFullUtilizationTest(3, 5, 900);
    }
}
