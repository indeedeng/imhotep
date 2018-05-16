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

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Decides which TaskQueue task should execute next
 */
public class TaskScheduler {
    // queue of tasks waiting to run
    private final Map<String, TaskQueue> usernameToQueue = Maps.newHashMap();
    // history of consumption of all tasks that ran recently
    private final Map<String, ConsumptionTracker> usernameToConsumptionTracker = Maps.newHashMap();
    private final Set<ImhotepTask> runningTasks = Sets.newHashSet();
    private final long totalSlots;
    private final long historyLengthNanos;
    private final long batchNanos;
    private final SchedulerType schedulerType;

    public static TaskScheduler CPUScheduler = new NoopTaskScheduler();
    public static TaskScheduler RemoteFSIOScheduler = new NoopTaskScheduler();
    public static TaskScheduler noopTaskScheduler = new NoopTaskScheduler();

    public TaskScheduler(long totalSlots, long historyLengthNanos, long batchNanos, SchedulerType schedulerType) {
        this.totalSlots = totalSlots;
        this.historyLengthNanos = historyLengthNanos;
        this.batchNanos = batchNanos;
        this.schedulerType = schedulerType;
    }

    /**
     * Blocks if necessary and returns once a slot is acquired.
     * Retrieves the ImhotepTask object from ThreadLocal. If absent results in a noop.
     * Must be used inside try-with-resources.
     */
    @Nullable
    public synchronized Closeable lockSlot() {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        if(task != null) {
            return new CloseableImhotepTask(task, this);
        } else {
            // TODO: add reporting on this
            return null; // can't lock with no task
        }
    }

    public synchronized Closeable lockSlotFromAnotherScheduler(final TaskScheduler schedulerToReleaseFrom) {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        if(task == null) {
            // TODO: add reporting on this
            return null; // can't lock with no task
        }

        final boolean otherSchedulerHadALock = schedulerToReleaseFrom.stopped(task);
        final Closeable newLock = lockSlot();
        if(!otherSchedulerHadALock) {
            return newLock;
        } else {
            return () -> {
                if (newLock != null) {
                    newLock.close();
                }
                schedulerToReleaseFrom.schedule(task);
            };
        }
    }

    /** returns true iff a new lock was created */
    synchronized boolean schedule(ImhotepTask task) {
        if(runningTasks.contains(task)) {
            return false;
        }
        task.preExecInitialize(this);
        final TaskQueue queue = getOrCreateQueueForTask(task);
        queue.offer(task);
        tryStartTasks();
        // Blocks and waits if necessary
        task.blockAndWait();
        return true;
    }

    /** returns true iff a task was running*/
    synchronized boolean stopped(ImhotepTask task) {
        if(!runningTasks.remove(task)) {
            return false;
        }
        final long consumption = task.stopped(schedulerType);
        ConsumptionTracker consumptionTracker = usernameToConsumptionTracker.get(task.userName);
        if(consumptionTracker == null) {
            consumptionTracker = new ConsumptionTracker(historyLengthNanos, batchNanos);
            usernameToConsumptionTracker.put(task.userName, consumptionTracker);
        }
        consumptionTracker.record(consumption);
        tryStartTasks();
        return true;
    }

    private synchronized void tryStartTasks() {
        if(runningTasks.size() >= totalSlots) {
            return; // fully utilized
        }
        for(TaskQueue taskQueue: usernameToQueue.values()) {
            taskQueue.updateConsumptionCache();
        }
        final PriorityQueue<TaskQueue> queues = Queues.newPriorityQueue(usernameToQueue.values());
        for(TaskQueue taskQueue : queues) {
            while(true) {
                final ImhotepTask queuedTask = taskQueue.poll();
                if(queuedTask == null) {
                    usernameToQueue.remove(taskQueue.getUsername());
                    break;
                }
                runningTasks.add(queuedTask);
                queuedTask.markRunnable(schedulerType);
                if(runningTasks.size() >= totalSlots) {
                    return;
                }
            }
        }
    }

    // TODO: run trim on all ConsumptionTrackers on a schedule and delete empty ones?

    // TODO: check runningTasks for leaks once in a while


    @Nonnull
    private synchronized TaskQueue getOrCreateQueueForTask(final ImhotepTask task) {
        TaskQueue queue = usernameToQueue.get(task.userName);
        if(queue == null) {
            ConsumptionTracker consumptionTracker = usernameToConsumptionTracker.get(task.userName);
            if(consumptionTracker == null) {
                consumptionTracker = new ConsumptionTracker(historyLengthNanos, batchNanos);
                usernameToConsumptionTracker.put(task.userName, consumptionTracker);
            }
            queue = new TaskQueue(task.userName, consumptionTracker);
            usernameToQueue.put(task.userName, queue);
        }
        return queue;
    }
}
