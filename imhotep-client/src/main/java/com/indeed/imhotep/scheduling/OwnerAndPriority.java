package com.indeed.imhotep.scheduling;

import java.util.Objects;

/**
 *  Contains a task owner and the task priority.
 *  The higher the priority number the higher the priority.
 */
public class OwnerAndPriority {
    public final String owner;
    public final byte priority;

    public OwnerAndPriority(String owner, byte priority) {
        this.owner = owner;
        this.priority = priority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OwnerAndPriority that = (OwnerAndPriority) o;
        return priority == that.priority &&
                Objects.equals(owner, that.owner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(owner, priority);
    }


}
