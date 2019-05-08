package com.indeed.imhotep;

import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.protobuf.SlotTimingMessage;
import com.indeed.imhotep.scheduling.SchedulerType;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jwolfe
 */
public class SlotTiming {
    private final AtomicLong cpuTimeNs = new AtomicLong(0);
    private final AtomicLong cpuWaitNs = new AtomicLong(0);
    private final AtomicLong ioTimeNs = new AtomicLong(0);
    private final AtomicLong ioWaitNs = new AtomicLong(0);

    public void schedulerExecTimeCallback(SchedulerType schedulerType, long execTime) {
        switch(schedulerType) {
            case CPU:
                cpuTimeNs.addAndGet(execTime);
                break;
            case REMOTE_FS_IO:
                ioTimeNs.addAndGet(execTime);
                break;
        }
    }

    public void schedulerWaitTimeCallback(SchedulerType schedulerType, long waitTime) {
        switch(schedulerType) {
            case CPU:
                cpuWaitNs.addAndGet(waitTime);
                break;
            case REMOTE_FS_IO:
                ioWaitNs.addAndGet(waitTime);
                break;
        }
    }

    public void writeToPerformanceStats(PerformanceStats.Builder performanceStats) {
        performanceStats.setCpuSlotsExecTimeMs(TimeUnit.NANOSECONDS.toMillis(cpuTimeNs.get()));
        performanceStats.setCpuSlotsWaitTimeMs(TimeUnit.NANOSECONDS.toMillis(cpuWaitNs.get()));
        performanceStats.setIoSlotsExecTimeMs(TimeUnit.NANOSECONDS.toMillis(ioTimeNs.get()));
        performanceStats.setIoSlotsWaitTimeMs(TimeUnit.NANOSECONDS.toMillis(ioWaitNs.get()));
    }

    public void addFromSlotTimingMessage(final SlotTimingMessage slotTimingMessage) {
        cpuTimeNs.addAndGet(slotTimingMessage.getCpuSlotsExecTimeNs());
        cpuWaitNs.addAndGet(slotTimingMessage.getCpuSlotsWaitTimeNs());
        ioTimeNs.addAndGet(slotTimingMessage.getIoSlotsExecTimeNs());
        ioWaitNs.addAndGet(slotTimingMessage.getIoSlotsWaitTimeNs());
    }

    public SlotTimingMessage writeToSlotTimingMessage() {
        final SlotTimingMessage.Builder builder = SlotTimingMessage.newBuilder();
        builder.setCpuSlotsExecTimeNs(cpuTimeNs.get());
        builder.setCpuSlotsWaitTimeNs(cpuWaitNs.get());
        builder.setIoSlotsExecTimeNs(ioTimeNs.get());
        builder.setIoSlotsWaitTimeNs(ioWaitNs.get());
        return builder.build();
    }

    public void addTimingProperties(final Map<String, Object> properties) {
        properties.put(Instrumentation.Keys.CPU_SLOTS_EXEC_TIME_MILLIS, TimeUnit.NANOSECONDS.toMillis(cpuTimeNs.get()));
        properties.put(Instrumentation.Keys.CPU_SLOTS_WAIT_TIME_MILLIS, TimeUnit.NANOSECONDS.toMillis(cpuWaitNs.get()));
        properties.put(Instrumentation.Keys.IO_SLOTS_EXEC_TIME_MILLIS, TimeUnit.NANOSECONDS.toMillis(ioTimeNs.get()));
        properties.put(Instrumentation.Keys.IO_SLOTS_WAIT_TIME_MILLIS, TimeUnit.NANOSECONDS.toMillis(ioWaitNs.get()));
    }

    public void reset() {
        cpuTimeNs.set(0);
        cpuWaitNs.set(0);
        ioTimeNs.set(0);
        ioWaitNs.set(0);
    }

    public long getCpuTimeNs() {
        return cpuTimeNs.get();
    }

    public long getCpuWaitNs() {
        return cpuWaitNs.get();
    }

    public long getIoTimeNs() {
        return ioTimeNs.get();
    }

    public long getIoWaitNs() {
        return ioWaitNs.get();
    }
}
