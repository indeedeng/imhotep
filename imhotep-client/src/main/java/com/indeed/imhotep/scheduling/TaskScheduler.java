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
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.threads.NamedThreadFactory;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
    private final MetricStatsEmitter statsEmitter;

    public static TaskScheduler CPUScheduler = new NoopTaskScheduler();
    public static TaskScheduler RemoteFSIOScheduler = new NoopTaskScheduler();

    private static final int REPORTING_FREQUENCY_MILLIS = 100;
    private static final int CLEANUP_FREQUENCY_MILLIS = 1000;
    private ScheduledExecutorService statsReportingExecutor = null;
    private ScheduledExecutorService cleanupExecutor = null;

    public TaskScheduler(long totalSlots, long historyLengthNanos, long batchNanos, SchedulerType schedulerType, MetricStatsEmitter statsEmitter) {
        this.totalSlots = totalSlots;
        this.historyLengthNanos = historyLengthNanos;
        this.batchNanos = batchNanos;
        this.schedulerType = schedulerType;
        this.statsEmitter = statsEmitter;

        initializeSchedulers(schedulerType);
    }

    protected void initializeSchedulers(SchedulerType schedulerType) {
        statsReportingExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("schedulerStatsReporter-" + schedulerType));
        statsReportingExecutor.scheduleAtFixedRate(this::reportStats, REPORTING_FREQUENCY_MILLIS, REPORTING_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);

        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("schedulerCleanup-" + schedulerType));
        cleanupExecutor.scheduleAtFixedRate(this::cleanup, CLEANUP_FREQUENCY_MILLIS, CLEANUP_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);
    }

    private void reportStats() {
        long waitingUsersCount = 0;
        long waitingTasksCount = 0;
        long runningTasksCount = 0;
        long longestWaitingTaskNanos = 0;
        long longestRunningTaskNanos = 0;
        synchronized (this){
            for (TaskQueue taskQueue : usernameToQueue.values()) {
                waitingTasksCount += taskQueue.size();
                waitingUsersCount += 1;
            }
            runningTasksCount = runningTasks.size();
            final long nowNanos = System.nanoTime();

            final Optional<Long> minRunStartTimeNanos = runningTasks.stream().map(ImhotepTask::getLastExecutionStartTime).min(Long::compareTo);
            long oldestRunStartTimeNanos = minRunStartTimeNanos.orElse(nowNanos);
            longestRunningTaskNanos = nowNanos - oldestRunStartTimeNanos;

            long minWaitStartTimeNanos = nowNanos;
            for(TaskQueue taskQueue: usernameToQueue.values()) {
                final ImhotepTask oldestTask = taskQueue.peek();
                if (oldestTask != null) {
                    minWaitStartTimeNanos = Math.min(minWaitStartTimeNanos, oldestTask.getLastWaitStartTime());
                }
            }
            longestWaitingTaskNanos = nowNanos - minWaitStartTimeNanos;
        }
        statsEmitter.histogram("scheduler." + schedulerType + ".longest.running", TimeUnit.NANOSECONDS.toMillis(longestRunningTaskNanos));
        statsEmitter.histogram("scheduler." + schedulerType + ".longest.waiting",  TimeUnit.NANOSECONDS.toMillis(longestWaitingTaskNanos));
        statsEmitter.histogram("scheduler." + schedulerType + ".waiting.users", waitingUsersCount);
        statsEmitter.histogram("scheduler." + schedulerType + ".waiting.tasks", waitingTasksCount);
        statsEmitter.histogram("scheduler." + schedulerType + ".running.tasks", runningTasksCount);
    }

    private void cleanup() {
        usernameToConsumptionTracker.entrySet().removeIf(entry -> !entry.getValue().isActive());
        // TODO: check runningTasks for leaks once in a while
    }

    /**
     * Blocks if necessary and returns once a slot is acquired.
     * Retrieves the ImhotepTask object from ThreadLocal. If absent results in a noop.
     * Must be used inside try-with-resources.
     */
    @Nonnull
    public SilentCloseable lockSlot() {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        if(task != null) {
            return new CloseableImhotepTask(task, this);
        } else {
            statsEmitter.count("scheduler." + schedulerType + ".threadlocal.task.absent", 1);
            return () -> {}; // can't lock with no task
        }
    }

    @Nonnull
    public Closeable temporaryUnlock() {
        final ImhotepTask task = ImhotepTask.THREAD_LOCAL_TASK.get();
        if(task == null) {
            statsEmitter.count("scheduler." + schedulerType + ".threadlocal.task.absent", 1);
            return () -> {}; // can't lock with no task
        }

        final boolean hadAlock = stopped(task);
        if(!hadAlock) {
            return () -> {};
        } else {
            return new Closeable() {
                boolean closed = false;
                @Override
                public void close() throws IOException {
                    if(closed) return;
                    closed = true;
                    schedule(task);
                }
            };
        }
    }

    /** returns true iff a new lock was created */
    boolean schedule(ImhotepTask task) {
        synchronized (this) {
            if (runningTasks.contains(task)) {
                statsEmitter.count("scheduler." + schedulerType + ".schedule.already.running", 1);
                return false;
            }
            task.preExecInitialize(this);
            final TaskQueue queue = getOrCreateQueueForTask(task);
            queue.offer(task);
            tryStartTasks();
        }
        // Blocks and waits if necessary
        task.blockAndWait();
        return true;
    }

    /** returns true iff a task was running */
    synchronized boolean stopped(ImhotepTask task) {
        if(!runningTasks.remove(task)) {
            statsEmitter.count("scheduler." + schedulerType + ".stop.already.stopped", 1);
            return false;
        }
        final long consumption = task.stopped(schedulerType);
        final ConsumptionTracker consumptionTracker = usernameToConsumptionTracker.computeIfAbsent(task.userName,
                (ignored) -> new ConsumptionTracker(historyLengthNanos, batchNanos));
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
        while(!queues.isEmpty()) {
            final TaskQueue taskQueue = queues.poll();
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

    public ArrayList<TaskServletUtil> getRunningTasksServlet() {
        synchronized (this) {
            final ArrayList<TaskServletUtil> allRunningTasks = new ArrayList<TaskServletUtil>();
            for (ImhotepTask task: runningTasks) {
                allRunningTasks.add(task.getTaskServletFields());
            }
            return allRunningTasks;
        }
    }

    @Nonnull
    private synchronized TaskQueue getOrCreateQueueForTask(final ImhotepTask task) {
        TaskQueue queue = usernameToQueue.get(task.userName);
        if(queue == null) {
            final ConsumptionTracker consumptionTracker = usernameToConsumptionTracker.computeIfAbsent(task.userName,
                    (ignored) -> new ConsumptionTracker(historyLengthNanos, batchNanos));
            queue = new TaskQueue(task.userName, consumptionTracker);
            usernameToQueue.put(task.userName, queue);
        }
        return queue;
    }

    public void close() {
        if(statsReportingExecutor != null) {
            statsReportingExecutor.shutdown();
        }
        if(cleanupExecutor != null) {
            cleanupExecutor.shutdown();
        }
    }

    public String getSchedulerTypeName() {
        return schedulerType.toString();
    }
}
