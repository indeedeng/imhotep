/*
 * Copyright (C) 2014 Indeed Inc.
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
package com.indeed.imhotep.io;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Extension of the BufferedOutputStream that throws an WriteLimitExceededException
 * when the number of bytes written exceeds the provided limit.
 * @author vladimir
 */
public class LimitedBufferedOutputStream extends BufferedOutputStream {
    @Nullable
    private final AtomicLong maxBytesToWrite;

    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream.
     * Throws a WriteLimitExceededException if written more than maxBytesToWrite bytes.
     *
     * @param   out   the underlying output stream.
     * @param   maxBytesToWrite    byte limit as an AtomicLong can be shared between multiple
     *                             LimitedBufferedOutputStreams running in parallel threads. If null, no limit is applied.
     */
    public LimitedBufferedOutputStream(final OutputStream out, @Nullable final AtomicLong maxBytesToWrite) {
        this(out, maxBytesToWrite, 8192);
    }

    /**
     * Creates a new buffered output stream to write data to the
     * specified underlying output stream with the specified buffer
     * size.
     * Throws a WriteLimitExceededException if written more than maxBytesToWrite bytes.
     *
     * @param   out    the underlying output stream.
     * @param   maxBytesToWrite    byte limit as an AtomicLong can be shared between multiple
     *                             LimitedBufferedOutputStreams running in parallel threads. If null, no limit is applied.
     * @param   size   the buffer size.
     * @exception IllegalArgumentException if size &lt;= 0.
     */
    public LimitedBufferedOutputStream(final OutputStream out, @Nullable final AtomicLong maxBytesToWrite, final int size) {
        super(out, size);
        this.maxBytesToWrite = maxBytesToWrite;
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        if(maxBytesToWrite != null && maxBytesToWrite.decrementAndGet() < 0) {
            throw new WriteLimitExceededException();
        }
        super.write(b);
    }

    @Override
    public synchronized void write(@Nonnull final byte[] b, final int off, final int len) throws IOException {
        if(maxBytesToWrite != null && maxBytesToWrite.addAndGet(-len) < 0) {
            throw new WriteLimitExceededException();
        }
        super.write(b, off, len);
    }
}
