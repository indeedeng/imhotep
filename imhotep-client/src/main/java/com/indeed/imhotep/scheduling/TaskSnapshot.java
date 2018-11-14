package com.indeed.imhotep.scheduling;

import com.indeed.imhotep.AbstractImhotepMultiSession;
import com.indeed.imhotep.AbstractImhotepSession;
import com.indeed.imhotep.RequestContext;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Class to hold all the fields of ImhotepTask to be reported in the Task Sevlet
 */

public class TaskSnapshot {

    public final long taskID;
    public final String sessionID;
    @Nullable public final String shardPath;
    @Nullable public final RequestContext requestContext;
    @Nullable public final Long numDocs;
    @Nullable public final Integer numStats;
    public final Duration timeSinceCreation;
    public final String userName;
    public final String clientName;
    private final String dataset;
    private final String shardName;
    public final Duration timeSinceLastExecutionStart;
    public final Duration timeSinceLastWaitStart;
    public final long totalExecutionTimeMillis;

    public TaskSnapshot(
            final long taskID,
            final @Nullable AbstractImhotepMultiSession session,
            final @Nullable AbstractImhotepSession innerSession,
            final @Nullable RequestContext requestContext,
            final long creationTime,
            @Nullable final String userName,
            @Nullable final String clientName,
            @Nullable final String dataset,
            @Nullable final String shardName,
            @Nullable final Integer numDocs,
            final long lastExecutionStartTime,
            final long lastWaitStartTime,
            final long totalExecutionTime
    ) {
        this.taskID = taskID;
        this.sessionID = ( (session == null) ? "null" : session.getSessionId() );
        this.shardPath = (innerSession == null) ? null : innerSession.getShardPath().toString();
        this.requestContext = requestContext;
        this.numDocs = (innerSession == null) ? ((numDocs != null) ? (long)(int)numDocs : null) : (Long)innerSession.getNumDocs();
        this.numStats = (innerSession == null) ? null : innerSession.getNumStats();
        this.timeSinceCreation = Duration.ZERO.plusNanos(System.nanoTime() - creationTime);
        this.userName = userName;
        this.clientName = clientName;
        this.dataset = dataset;
        this.shardName = shardName;
        this.timeSinceLastExecutionStart = Duration.ZERO.plusNanos(System.nanoTime() - lastExecutionStartTime);
        this.timeSinceLastWaitStart = Duration.ZERO.plusNanos(System.nanoTime() - lastWaitStartTime);
        this.totalExecutionTimeMillis = TimeUnit.MILLISECONDS.convert((totalExecutionTime + System.nanoTime() - lastExecutionStartTime), TimeUnit.NANOSECONDS);
    }

    public String getTimeSinceCreation() {
        return this.timeSinceCreation.toString();
    }

    public String getTimeSinceLastExecutionStart() {
        return this.timeSinceLastExecutionStart.toString();
    }

    public String getTimeSinceLastWaitStart() {
        return this.timeSinceLastWaitStart.toString();
    }
}
