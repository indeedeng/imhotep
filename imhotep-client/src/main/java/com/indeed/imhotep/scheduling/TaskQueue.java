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
import com.google.common.primitives.Longs;

import javax.annotation.Nonnull;
import java.util.PriorityQueue;

/**
 * Represents a queue of tasks that should run in FIFO order.
 */
public class TaskQueue implements Comparable<TaskQueue> {
    private final PriorityQueue<ImhotepTask> queue = Queues.newPriorityQueue();
    private final String username;
    private final ConsumptionTracker consumptionTracker;
    private long cachedConsumption;

    public TaskQueue(String username, ConsumptionTracker consumptionTracker) {
        this.username = username;
        this.consumptionTracker = consumptionTracker;
    }

    public ImhotepTask poll() {
        return queue.poll();
    }

    public ImhotepTask peek() {
        return queue.peek();
    }

    public void offer(ImhotepTask task) {
        queue.offer(task);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public String getUsername() {
        return username;
    }

    public int size() {
        return queue.size();
    }

    public void updateConsumptionCache() {
        cachedConsumption = consumptionTracker.getConsumption();
    }

    /** Used to prioritize queues with lower consumption in the TaskScheduler */
    @Override
    public int compareTo(@Nonnull TaskQueue o) {
        return Longs.compare(cachedConsumption, o.cachedConsumption);
    }
}
