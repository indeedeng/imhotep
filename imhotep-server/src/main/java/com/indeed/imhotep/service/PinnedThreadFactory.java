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
package com.indeed.imhotep.service;

import javax.annotation.Nonnull;
import java.util.concurrent.ThreadFactory;

class PinnedThreadFactory implements ThreadFactory {

    private final int cpu;

    public PinnedThreadFactory(final int cpu) {
        this.cpu = cpu;
    }

    @Override
    public Thread newThread(@Nonnull final Runnable runnable) {
        return new PinnedThread(runnable);
    }

    private class PinnedThread extends Thread {

        public PinnedThread(final Runnable runnable) {
            super(runnable);
        }

        public void run() {
            pin(PinnedThreadFactory.this.cpu);
            super.run();
        }

        private native void pin(final int cpu);
    }
}
