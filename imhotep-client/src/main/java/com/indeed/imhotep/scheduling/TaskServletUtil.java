package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.AbstractImhotepMultiSession;
import java.util.Date;
import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold all the fields of ImhotepTask to be reported in the Task Sevlet
 */

public class TaskServletUtil {

    private final long taskID;
    private final String sessionID;
    private final Date creationTime;
    private final String userName;
    private final String clientName;
    private final Date lastExecutionStartTime;
    private final Date lastWaitStartTime;
    private long totalExecutionTimeMillis;
    private String schedulerType;

    public TaskServletUtil(
            long taskID,
            @Nullable AbstractImhotepMultiSession session,
            long creationTime,
            String userName,
            String clientName,
            long lastExecutionStartTime,
            long lastWaitStartTime,
            long totalExecutionTime,
            String schedulerType) {
        this.taskID = taskID;
        this.sessionID = ( (session == null) ? "null" : session.getSessionId() );
        this.creationTime = new Date(creationTime);
        this.userName = userName;
        this.clientName = clientName;
        this.lastExecutionStartTime = new Date(lastExecutionStartTime);
        this.lastWaitStartTime = new Date(lastWaitStartTime);
        this.totalExecutionTimeMillis = TimeUnit.MILLISECONDS.convert(totalExecutionTime, TimeUnit.NANOSECONDS);
        this.schedulerType = schedulerType;
    }

    public long getTaskID() {
        return this.taskID;
    }

    public String getSessionID() {
        return this.sessionID;
    }


    public Date getCreationTime() {
        return this.creationTime;
    }

    public Date getLastExecutionStartTime() {
        return this.lastExecutionStartTime;
    }

    public Date getLastWaitStartTime() {
        return this.lastWaitStartTime;
    }

    public long getTotalExecutionTimeMillis() {
        return this.totalExecutionTimeMillis;
    }

    public String getUsername() {
        return this.userName;
    }

    public String getClientName() {
        return this.clientName;
    }

    public String getSchedulerType() {
        return this.schedulerType;
    }

}
