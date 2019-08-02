package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.exceptions.InvalidSessionException;
import com.indeed.imhotep.service.MetricStatsEmitter;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.when;

public class TestImhotepTaskCancellation {
    @Test
    public void testSessionClose() {
        final int oneSecInMillis = (int) TimeUnit.SECONDS.toMillis(1);
        final long oneSecInNanos = TimeUnit.SECONDS.toNanos(1);
        try (final TaskScheduler taskScheduler = new TaskScheduler(1, oneSecInNanos, oneSecInNanos, oneSecInMillis, SchedulerType.CPU,  MetricStatsEmitter.NULL_EMITTER)) {
            final AbstractImhotepMultiSession session = Mockito.mock(AbstractImhotepMultiSession.class);
            when(session.getUserName()).thenReturn("user");
            when(session.getClientName()).thenReturn("client");
            when(session.getPriority()).thenReturn((byte) 0);
            final AtomicBoolean closed = new AtomicBoolean(false);
            when(session.isClosed()).thenAnswer((Answer<Boolean>) ignored -> closed.get());
            ImhotepTask.setup(session);
            try (final SilentCloseable ignored = taskScheduler.lockSlot()) {

                ImhotepTask.THREAD_LOCAL_TASK.get().changeTaskStartTime(2 * oneSecInNanos);
                taskScheduler.yieldIfNecessary();

                closed.set(true);
                ImhotepTask.THREAD_LOCAL_TASK.get().changeTaskStartTime(2 * oneSecInNanos);
                try {
                    taskScheduler.yieldIfNecessary();
                    Assert.fail("Expected an InvalidSessionException but it didn't throw");
                } catch (final InvalidSessionException e) {
                    // Expected
                } catch (final Throwable e) {
                    Assert.fail("Expected an InvalidSessionException but got " + e);
                }
            }
        }
    }
}