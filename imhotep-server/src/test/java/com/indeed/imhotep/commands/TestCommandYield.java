package com.indeed.imhotep.commands;

import com.google.common.base.Throwables;
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
import com.indeed.imhotep.service.TestCachedFlamdexReader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestCommandYield {

    private List<String> scheduleOrder = new ArrayList<>();

    private static String USER1 = "userName1";
    private static String CLIENT1 = "ClientName1";
    private static String USER2 = "userName2";
    private static String CLIENT2 = "ClientName2";
    private static String COMMANDID1 = "commandId1";
    private static String COMMANDID2 = "commandId2";
    private static String SESSIONID = "randomSessionIdString";

    private ImhotepCommand getSleepingCommand(final long milliSeconds, final String commandId) {
        return new VoidAbstractImhotepCommand(SESSIONID) {
            @Override
            public void applyVoid(final ImhotepSession imhotepSession) throws ImhotepOutOfMemoryException {
                synchronized (scheduleOrder) {
                    scheduleOrder.add(commandId);
                }
                ImhotepTask.THREAD_LOCAL_TASK.get().overritdeTaskStartTime(System.nanoTime() - milliSeconds*1000000L);
            }

            @Override
            protected RequestTools.ImhotepRequestSender imhotepRequestSenderInitializer() {
                return null;
            }
        };
    }

    @Before
    public void setup() {
        TaskScheduler.CPUScheduler = new TaskScheduler(1, TimeUnit.SECONDS.toNanos(60) , TimeUnit.SECONDS.toNanos(1), SchedulerType.CPU, MetricStatsEmitter.NULL_EMITTER);

        ImhotepTask.setup(USER1, CLIENT1, (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
            ImhotepTask.THREAD_LOCAL_TASK.get().overritdeTaskStartTime(System.nanoTime() - 1000000000L);
        }

        ImhotepTask.setup(USER2, CLIENT2, (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
        }
        ImhotepTask.clear();
    }

    @After
    public void clear() {
        TaskScheduler.CPUScheduler.close();
    }

    public Thread getCommandThread(final String username, final String clientName, final String commandID, long... commandsExecTimeMillis) {
        return new Thread(() -> {
            try {
                final ImhotepLocalSession imhotepLocalSession = new ImhotepJavaLocalSession(SESSIONID, new TestCachedFlamdexReader.SillyFlamdexReader(), null );

                final List<ImhotepCommand> firstCommands = new ArrayList<>();
                for (int i = 0; i < commandsExecTimeMillis.length - 1; i++) {
                    firstCommands.add(getSleepingCommand(commandsExecTimeMillis[i], commandID));
                }

                final ImhotepCommand lastCommand = getSleepingCommand(commandsExecTimeMillis[commandsExecTimeMillis.length - 1], commandID);

                ImhotepTask.setup(username, clientName, (byte)0, new SlotTiming());
                try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
                    imhotepLocalSession.executeBatchRequest(firstCommands, lastCommand);
                }

            } catch (ImhotepOutOfMemoryException e) {
                Throwables.propagate(e);
            }
        });
    }

    private void executeThreads(final Thread thread1, final Thread thread2) throws InterruptedException {
        ImhotepTask.setup("TestUsername", "testClient", (byte) 0, new SlotTiming());
        try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
            thread1.start();
            thread2.start();
            Thread.sleep(100L);
        }

        thread1.join();
        thread2.join();
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
}
