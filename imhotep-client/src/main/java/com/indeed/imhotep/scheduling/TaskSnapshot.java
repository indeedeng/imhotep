package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.AbstractImhotepMultiSession;

import java.time.Duration;
import java.util.Date;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold all the fields of ImhotepTask to be reported in the Task Sevlet
 */

public class TaskSnapshot {

    public final long taskID;
    public final String sessionID;
    public final String timeSinceCreation;
    public final String userName;
    public final String clientName;
    public final String timeSinceLastExecutionStart;
    public final String timeSinceLastWaitStart;
    public final long totalExecutionTimeMillis;
    public final SchedulerType schedulerType;

    public TaskSnapshot (
            long taskID,
            @Nullable AbstractImhotepMultiSession session,
            long creationTime,
            String userName,
            String clientName,
            long lastExecutionStartTime,
            long lastWaitStartTime,
            long totalExecutionTime,
            SchedulerType schedulerType) {
        this.taskID = taskID;
        this.sessionID = ( (session == null) ? "null" : session.getSessionId() );
        this.timeSinceCreation = Duration.ZERO.plusNanos(System.nanoTime() - creationTime).toString();
        this.userName = userName;
        this.clientName = clientName;
        this.timeSinceLastExecutionStart = Duration.ZERO.plusNanos(System.nanoTime() - lastExecutionStartTime).toString();
        this.timeSinceLastWaitStart = Duration.ZERO.plusNanos(System.nanoTime() - lastWaitStartTime).toString();
        this.totalExecutionTimeMillis = TimeUnit.MILLISECONDS.convert((totalExecutionTime + System.nanoTime() - lastExecutionStartTime), TimeUnit.NANOSECONDS);
        this.schedulerType = schedulerType;
    }

}
