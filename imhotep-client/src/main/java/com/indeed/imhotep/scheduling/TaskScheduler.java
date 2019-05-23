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

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.indeed.imhotep.service.MetricStatsEmitter;
import com.indeed.util.core.threads.NamedThreadFactory;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Decides which TaskQueue task should execute next
 */
public class TaskScheduler {
    private static final Logger LOGGER = Logger.getLogger(TaskScheduler.class);

    private static final long LONG_RUNNING_TASK_THRESHOLD_MILLIS = TimeUnit.MINUTES.toMillis(5);
    private static final int DATADOG_STATS_REPORTING_FREQUENCY_MILLIS = 100;
    private static final int CLEANUP_FREQUENCY_MILLIS = 1000;
    private static final long LONG_RUNNING_TASK_REPORT_FREQUENCY_MILLIS = TimeUnit.MINUTES.toMillis(1);

    // queue of tasks waiting to run
    private final Map<OwnerAndPriority, TaskQueue> queues = Maps.newHashMap();
    // history of consumption of all tasks that ran recently
    private final Map<OwnerAndPriority, ConsumptionTracker> ownerToConsumptionTracker = Maps.newHashMap();
    private final Set<ImhotepTask> runningTasks = ConcurrentHashMap.newKeySet();
    private final int totalSlots;
    private final long historyLengthNanos;
    private final long batchNanos;
    private final SchedulerType schedulerType;
    private final MetricStatsEmitter statsEmitter;

    public static TaskScheduler CPUScheduler = new NoopTaskScheduler();
    public static TaskScheduler RemoteFSIOScheduler = new NoopTaskScheduler();

    private ScheduledExecutorService datadogStatsReportingExecutor = null;
    private ScheduledExecutorService cleanupExecutor = null;
    private ScheduledExecutorService longRunningTaskReportingExecutor = null;

    public TaskScheduler(int totalSlots, long historyLengthNanos, long batchNanos, SchedulerType schedulerType, MetricStatsEmitter statsEmitter) {
        this.totalSlots = totalSlots;
        this.historyLengthNanos = historyLengthNanos;
        this.batchNanos = batchNanos;
        this.schedulerType = schedulerType;
        this.statsEmitter = statsEmitter;

        initializeSchedulers(schedulerType);
    }

    protected void initializeSchedulers(final SchedulerType schedulerType) {
        datadogStatsReportingExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("schedulerDatadogStatsReporter-" + schedulerType));
        datadogStatsReportingExecutor.scheduleAtFixedRate(this::reportDatadogStats, DATADOG_STATS_REPORTING_FREQUENCY_MILLIS, DATADOG_STATS_REPORTING_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);

        cleanupExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("schedulerCleanup-" + schedulerType));
        cleanupExecutor.scheduleAtFixedRate(this::cleanup, CLEANUP_FREQUENCY_MILLIS, CLEANUP_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);

        longRunningTaskReportingExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("schedulerLongRunningTaskReporter-" + schedulerType));
        longRunningTaskReportingExecutor.scheduleAtFixedRate(this::reportLongRunningTasks, LONG_RUNNING_TASK_REPORT_FREQUENCY_MILLIS, LONG_RUNNING_TASK_REPORT_FREQUENCY_MILLIS, TimeUnit.MILLISECONDS);
    }

    public int getTotalSlots() {
        return totalSlots;
    }

    private void reportDatadogStats() {
        long waitingUsersCount = 0;
        long waitingTasksCount = 0;
        long runningTasksCount = 0;
        long longestWaitingTaskNanos = 0;
        long longestRunningTaskNanos = 0;
        synchronized (this){
            for (TaskQueue taskQueue : queues.values()) {
                waitingTasksCount += taskQueue.size();
                waitingUsersCount += 1;
            }
            runningTasksCount = runningTasks.size();
            final long nowNanos = System.nanoTime();

            final Optional<Long> minRunStartTimeNanos = runningTasks.stream().map(ImhotepTask::getLastExecutionStartTime).min(Long::compareTo);
            long oldestRunStartTimeNanos = minRunStartTimeNanos.orElse(nowNanos);
            longestRunningTaskNanos = nowNanos - oldestRunStartTimeNanos;

            long minWaitStartTimeNanos = nowNanos;
            for (TaskQueue taskQueue : queues.values()) {
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
        ownerToConsumptionTracker.entrySet().removeIf(entry -> !entry.getValue().isActive());
        // TODO: check runningTasks for leaks once in a while
    }

    private void reportLongRunningTasks() {
        for (final ImhotepTask runningTask : runningTasks) {
            final long currentExecutionTime = TimeUnit.NANOSECONDS.toMillis(runningTask.getCurrentExecutionTime().orElse(0));
            if (currentExecutionTime >= LONG_RUNNING_TASK_THRESHOLD_MILLIS) {
                final StackTraceElement[] stackTraceElements;
                try {
                    stackTraceElements = runningTask.getStackTrace();
                } catch (final Exception e) {
                    LOGGER.error("Failed to take the stack trace of " + runningTask);
                    continue;
                }

                LOGGER.info(
                        "Imhotep task " + runningTask + " for scheduler " + schedulerType + " running a long time. stack trace:\n  "
                                + Joiner.on("\n  ").join(stackTraceElements)
                );
            }
        }
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

        final boolean hadAlock = stopped(task, true);
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
        final QueuedImhotepTask queuedTask = new QueuedImhotepTask(task);
        synchronized (this) {
            if (runningTasks.contains(task)) {
                statsEmitter.count("scheduler." + schedulerType + ".schedule.already.running", 1);
                return false;
            }
            task.preExecInitialize(this);
            final TaskQueue queue = getOrCreateQueueForTask(task);
            queue.offer(queuedTask);
            tryStartTasks();
        }

        try {
            // Blocks and waits if necessary
            task.blockAndWait();
        } catch (final Throwable t) {
            stopOrCancelTask(queuedTask, t);
            throw t;
        }
        return true;
    }

    /** returns true iff a task was running */
    synchronized boolean stopped(ImhotepTask task, boolean ignoreNotRunning) {
        if(!runningTasks.remove(task)) {
            if(!ignoreNotRunning) {
                statsEmitter.count("scheduler." + schedulerType + ".stop.already.stopped", 1);
            }
            return false;
        }
        final long consumption = task.stopped(schedulerType);
        final OwnerAndPriority ownerAndPriority = new OwnerAndPriority(task.userName, task.priority);
        final ConsumptionTracker consumptionTracker = ownerToConsumptionTracker.computeIfAbsent(ownerAndPriority,
                (ignored) -> new ConsumptionTracker(historyLengthNanos, batchNanos));
        consumptionTracker.record(consumption);
        tryStartTasks();
        return true;
    }

    private synchronized void tryStartTasks() {
        if(runningTasks.size() >= totalSlots) {
            return; // fully utilized
        }
        for(TaskQueue taskQueue: queues.values()) {
            taskQueue.updateConsumptionCache();
        }
        // prioritizes queues using TaskQueue.compareTo()
        final PriorityQueue<TaskQueue> prioritizedQueues = Queues.newPriorityQueue(queues.values());
        while(!prioritizedQueues.isEmpty()) {
            final TaskQueue taskQueue = prioritizedQueues.poll();
            while(true) {
                final QueuedImhotepTask queuedTask = taskQueue.poll();
                if(queuedTask == null) {
                    queues.remove(taskQueue.getOwnerAndPriority());
                    break;
                }

                if (queuedTask.cancelled) {
                    continue;
                }
                final ImhotepTask task = queuedTask.imhotepTask;
                try {
                    runningTasks.add(task);
                    task.markRunnable(schedulerType);
                    if(runningTasks.size() >= totalSlots) {
                        return;
                    }
                } catch (final Throwable t) {
                    stopOrCancelTask(queuedTask, t);
                }
            }
        }
    }

    private synchronized void stopOrCancelTask(@Nonnull final QueuedImhotepTask queuedTask, final Throwable t) {
        final ImhotepTask task = queuedTask.imhotepTask;
        if (runningTasks.contains(task)) {
            stopped(task, true);
            LOGGER.warn("Stopped the running task, task = " + task, t);
        } else {
            queuedTask.cancelled = true;
            LOGGER.warn("Cancelled task in the queue, task = " + task, t);
        }
    }

    public List<TaskSnapshot> getRunningTasksSnapshot(final boolean takeStackTrace) {
        synchronized (this) {
            return runningTasks.stream()
                    .map(task -> task.getSnapshot(takeStackTrace))
                    .collect(Collectors.toList());
        }
    }

    @Nonnull
    private synchronized TaskQueue getOrCreateQueueForTask(final ImhotepTask task) {
        final OwnerAndPriority ownerAndPriority = new OwnerAndPriority(task.userName, task.priority);
        TaskQueue queue = queues.get(ownerAndPriority);
        if(queue == null) {
            final ConsumptionTracker consumptionTracker = ownerToConsumptionTracker.computeIfAbsent(ownerAndPriority,
                    (ignored) -> new ConsumptionTracker(historyLengthNanos, batchNanos));
            queue = new TaskQueue(ownerAndPriority, consumptionTracker);
            queues.put(ownerAndPriority, queue);
        }
        return queue;
    }

    public void close() {
        if (datadogStatsReportingExecutor != null) {
            datadogStatsReportingExecutor.shutdown();
        }
        if (cleanupExecutor != null) {
            cleanupExecutor.shutdown();
        }
        if (longRunningTaskReportingExecutor != null) {
            longRunningTaskReportingExecutor.shutdown();
        }
    }

    public SchedulerType getSchedulerType() {
        return schedulerType;
    }
}
