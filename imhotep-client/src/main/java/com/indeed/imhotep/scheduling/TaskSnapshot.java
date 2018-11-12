package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.AbstractImhotepMultiSession;
import java.util.Date;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold all the fields of ImhotepTask to be reported in the Task Sevlet
 */

public class TaskSnapshot {

    public final long taskID;
    public final String sessionID;
    public final Date creationTime;
    public final String userName;
    public final String clientName;
    public final Date lastExecutionStartTime;
    public final Date lastWaitStartTime;
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
        this.creationTime = new Date(System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-creationTime));
        this.userName = userName;
        this.clientName = clientName;
        this.lastExecutionStartTime = new Date(System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-lastExecutionStartTime));
        this.lastWaitStartTime = new Date(System.currentTimeMillis() - TimeUnit.NANOSECONDS.toMillis(System.nanoTime()-lastWaitStartTime));
        this.totalExecutionTimeMillis = TimeUnit.MILLISECONDS.convert(totalExecutionTime, TimeUnit.NANOSECONDS);
        this.schedulerType = schedulerType;
    }

}
