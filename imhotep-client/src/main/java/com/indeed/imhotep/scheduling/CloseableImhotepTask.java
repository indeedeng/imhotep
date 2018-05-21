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

import java.io.Closeable;
import java.io.IOException;

/**
 * ImhotepTask wrapper to be used with try-with-resources
 */
class CloseableImhotepTask implements Closeable {

    private final ImhotepTask task;
    private final TaskScheduler taskScheduler;
    private boolean locked;

    CloseableImhotepTask(ImhotepTask task, TaskScheduler taskScheduler) {
        this.task = task;
        this.taskScheduler = taskScheduler;
        locked = taskScheduler.schedule(task);
    }

    @Override
    public void close() throws IOException {
        if(locked) {
            locked = false;
            taskScheduler.stopped(task);
        }
    }
}
