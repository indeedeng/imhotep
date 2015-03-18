package com.indeed.imhotep.multicache;

/**
 * Created by indeedLoan03 on 2/8/15.
 */
public interface ProcessingTaskFactory<D,R> {
    public ProcessingTask<D,R> newTask();
}

