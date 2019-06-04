package com.indeed.imhotep.scheduling;

import lombok.EqualsAndHashCode;

/**
 * @author xweng
 */
@EqualsAndHashCode
public class QueuedImhotepTask implements Comparable<QueuedImhotepTask>{
    public final ImhotepTask imhotepTask;

    @EqualsAndHashCode.Exclude
    public boolean cancelled;

    public QueuedImhotepTask(final ImhotepTask imhotepTask) {
        this.imhotepTask = imhotepTask;
        this.cancelled = false;
    }

    @Override
    public int compareTo(final QueuedImhotepTask other) {
        return imhotepTask.compareTo(other.imhotepTask);
    }
}
