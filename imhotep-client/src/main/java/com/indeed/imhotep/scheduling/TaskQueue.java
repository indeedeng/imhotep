/*
 * Copyright (C) 2018 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.indeed.imhotep.scheduling;

import com.google.common.collect.Queues;

import javax.annotation.Nonnull;
import java.util.PriorityQueue;

/**
 * Represents a queue of tasks that should run in FIFO order.
 */
public class TaskQueue implements Comparable<TaskQueue> {
    private final PriorityQueue<ImhotepTask> queue = Queues.newPriorityQueue();
    private final OwnerAndPriority ownerAndPriority;
    private final ConsumptionTracker consumptionTracker;
    private long cachedConsumption;

    public TaskQueue(OwnerAndPriority ownerAndPriority, ConsumptionTracker consumptionTracker) {
        this.ownerAndPriority = ownerAndPriority;
        this.consumptionTracker = consumptionTracker;
    }

    public ImhotepTask poll() {
        return queue.poll();
    }

    public ImhotepTask peek() {
        return queue.peek();
    }

    public void offer(final ImhotepTask queuedTask) {
        queue.offer(queuedTask);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public OwnerAndPriority getOwnerAndPriority() {
        return ownerAndPriority;
    }

    public int size() {
        return queue.size();
    }

    public void updateConsumptionCache(final long nanoTime) {
        cachedConsumption = consumptionTracker.getConsumption(nanoTime);
    }

    /** Used to prioritize queues with higher priority number or, if equal, with lower consumption in the TaskScheduler */
    @Override
    public int compareTo(@Nonnull TaskQueue o) {
        // negated as we want higher priority to win
        final int priorityDifference = -Byte.compare(ownerAndPriority.priority, o.ownerAndPriority.priority);
        if (priorityDifference != 0) {
            return priorityDifference;
        }
        return Long.compare(cachedConsumption, o.cachedConsumption);
    }
}
