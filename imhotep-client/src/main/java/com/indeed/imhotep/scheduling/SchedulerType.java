package com.indeed.imhotep.scheduling;

public enum SchedulerType {
    CPU,
    REMOTE_FS_IO,
    P2P_FS_IO;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
