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
 package com.indeed.imhotep;

import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

import java.io.Closeable;

/**
 * @author jplaisance
 */
public abstract class MemoryReserver implements Closeable {

    public abstract long usedMemory();

    public abstract long totalMemory();

    public enum AllocationResult {
        ALLOCATED("success"),
        EXCEEDS_GLOBAL_LIMIT("The machine ran out of memory. Please wait a few minutes and try again."),
        EXCEEDS_LOCAL_LIMIT("The query exceeded the memory allocated to it. Please reduce resource usage.");


        public final String msg;

        AllocationResult(final String msg) {
            this.msg = msg;
        }
    }

    public abstract AllocationResult claimMemory(long numBytes);

    public void claimMemoryOrThrowIOOME(long numBytes) throws ImhotepOutOfMemoryException {
        final AllocationResult allocationResult = claimMemory(numBytes);
        if (allocationResult != AllocationResult.ALLOCATED) {
            throw new ImhotepOutOfMemoryException(allocationResult.msg);
        }
    }

    public abstract void releaseMemory(long numBytes);

    public void releaseMemory(final MemoryMeasured object) {
        final long size = object.memoryUsed();
        object.close();
        releaseMemory(size);
    }

    @Override
    public abstract void close();
}
