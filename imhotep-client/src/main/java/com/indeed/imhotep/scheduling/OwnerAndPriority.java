package com.indeed.imhotep.scheduling;

import lombok.EqualsAndHashCode;

/**
 *  Contains a task owner and the task priority.
 *  The higher the priority number the higher the priority.
 */
@EqualsAndHashCode
public class OwnerAndPriority {
    public final String owner;
    public final byte priority;

    public OwnerAndPriority(String owner, byte priority) {
        this.owner = owner;
        this.priority = priority;
    }
}
