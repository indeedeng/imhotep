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

    private List<String> scheduleOrder = new ArrayList<>();

    private static String COMMANDID1 = "commandId1";
    private static String COMMANDID2 = "commandId2";
    private static String SESSIONID = "randomSessionIdString";

    private static final String[] USER = new String[]{"userName1", "userName2"};
    private static final String[] CLIENT = new String[]{"ClientName1", "ClientName2"};
    private static final String[] COMMAND = {COMMANDID1, COMMANDID2};
    private static final int[] initialPause = new int[] {1000, 0};

    private static TaskScheduler oldCPUScheduler = TaskScheduler.CPUScheduler;

    private ImhotepCommand getSleepingCommand(final long milliSeconds, final String commandId) {
        return new VoidAbstractImhotepCommand(SESSIONID) {
            @Override
            public void applyVoid(final ImhotepSession imhotepSession) {
                synchronized (scheduleOrder) {
                    scheduleOrder.add(commandId);
                }
                try {
                    Thread.sleep(milliSeconds);
                } catch (final InterruptedException ignored) {
                }
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
    }

    @After
    public void clear() {
        TaskScheduler.CPUScheduler = oldCPUScheduler;
        scheduleOrder.clear();
    }

    public Thread getCommandThread(final int index, long... commandsExecTimeMillis) {
        return new Thread(() -> {
            try {
                final ImhotepLocalSession imhotepLocalSession = new ImhotepJavaLocalSession(SESSIONID, new MemoryFlamdex(), null );

                final List<ImhotepCommand> firstCommands = new ArrayList<>();
                for (int i = 0; i < (commandsExecTimeMillis.length - 1); i++) {
                    firstCommands.add(getSleepingCommand(commandsExecTimeMillis[i], COMMAND[index]));
                }

                final ImhotepCommand lastCommand = getSleepingCommand(commandsExecTimeMillis[commandsExecTimeMillis.length - 1], COMMAND[index]);

                ImhotepTask.setup(USER[index], CLIENT[index], (byte)0, new SlotTiming());
                try {
                    Thread.sleep(initialPause[index]);
                } catch (final InterruptedException ignored) {
                }
                try (final SilentCloseable slot = TaskScheduler.CPUScheduler.lockSlot()) {
                    imhotepLocalSession.executeBatchRequest(firstCommands, lastCommand);
                }
                imhotepLocalSession.close();

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
        }

        thread1.join();
        thread2.join();
        ImhotepTask.clear();
    }

    @Test
    public void testInterleaving() throws InterruptedException {
        final Thread thread1 = getCommandThread(0, 0);
        final Thread thread2 = getCommandThread(1, 2000, 2000);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID1, COMMANDID2));
    }

    @Test
    public void testUnweave() throws InterruptedException {
        final Thread thread1 = getCommandThread(0, 0);
        final Thread thread2 = getCommandThread(1, 200L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID1));
    }

    @Test
    public void testLongInterleave() throws InterruptedException {
        final Thread thread1 = getCommandThread(0, 0);
        final Thread thread2 = getCommandThread(1, 100L, 100L, 900L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID2));
    }

    @Test
    public void testLongerInterleave() throws InterruptedException {
        final Thread thread1 = getCommandThread(0, 1000L, 200L);
        final Thread thread2 = getCommandThread(1, 100L, 100L, 900L, 2000L);
        executeThreads(thread1, thread2);

        /*
            Expected results explanation:
            At time 0, 100 and 200 commands from second thread is chosen since first thread is still sleeping.
            At time 1100 command finishes and command from first thread is chosen since it has lower consumption (0 vs 1100).
            At time 2100 command finishes and command from first thread is chosen again since it has lower consumption (1000 vs 1100)
            At time 2300 command finishes and command from second is chosen since first have no more commands.
         */
        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID1, COMMANDID2));
    }

    @Test
    public void testLongerInterleave1() throws InterruptedException {
        final Thread thread1 = getCommandThread(0, 100L, 100L);
        final Thread thread2 = getCommandThread(1, 100L, 100L, 1900L, 2000L);
        executeThreads(thread1, thread2);

        Assert.assertEquals(scheduleOrder, Arrays.asList(COMMANDID2, COMMANDID2, COMMANDID2, COMMANDID1, COMMANDID1, COMMANDID2));
    }
}
