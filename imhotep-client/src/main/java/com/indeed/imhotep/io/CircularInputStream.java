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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;

/**
 *
 * lock free circular buffer (except in the waiting for input/waiting for space states to avoid busy waiting)
 * this assumes a single reader and a single writer
 *
 * @author jplaisance
 */
public final class CircularInputStream extends InputStream {
    private static final Logger log = Logger.getLogger(CircularInputStream.class);

    private final byte[] buffer;

    private final int bufferMask;

    //empty if reader.pointer.value == writer.pointer.value
    //full if (writer.pointer.value+1) % buffer.length == reader.pointer.value

    private final State reader = new State();

    private final State writer;

    static final class State {
        private final PaddedVolatileInt pointer = new PaddedVolatileInt();
        private final PaddedVolatileBool closed = new PaddedVolatileBool();
        private final PaddedVolatileBool waiting;
        private final PaddedInt cache = new PaddedInt();
        private final Object lock;

        State() {
            this(new PaddedInt(), new PaddedVolatileBool());
        }

        State(final PaddedInt lock, final PaddedVolatileBool waiting) {
            this.lock = lock;
            this.waiting = waiting;
        }
    }


    // x86/sun 64 bit jdk alignment:
    // 16 bytes object header
    // 40 bytes padding
    // 8 bytes data
    // 64 bytes padding (to ensure header for next object which contains the futex
    // x86 currently uses 64 byte cache lines so it is impossible for the data to end up on the same cache line as anything else

    private static class PrePadding {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
    }

    private static class VolatileInt extends PrePadding {
        volatile int value;
        int pad;
    }

    private static class Int extends PrePadding {
        int value;
        int pad;
    }

    private static final class PaddedVolatileInt extends VolatileInt {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;
    }

    static final class PaddedInt extends Int {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;
    }

    private static class VolatileBool extends PrePadding {
        volatile boolean value;
        boolean pad1;
        boolean pad2;
        boolean pad3;
        boolean pad4;
        boolean pad5;
        boolean pad6;
        boolean pad7;
    }

    static final class PaddedVolatileBool extends VolatileBool {
        long pad1;
        long pad2;
        long pad3;
        long pad4;
        long pad5;
        long pad6;
        long pad7;
        long pad8;
    }

    public CircularInputStream(int bufferSize) {
        this(bufferSize, new State());
    }

    public CircularInputStream(int bufferSize, State writer) {
        if (bufferSize != Integer.lowestOneBit(bufferSize)) throw new IllegalArgumentException();
        buffer = new byte[bufferSize];
        bufferMask = bufferSize-1;
        this.writer = writer;
    }

    private int getTailFromCache(final int head) {
        int tail = writer.cache.value;
        if (head == tail) {
            tail = writer.pointer.value;
            writer.cache.value = tail;
        }
        return tail;
    }

    public int read() throws IOException {
        while (true) {
            final int head = reader.pointer.value;
            final int tail = getTailFromCache(head);
            if (head != tail) {
                final int ret = buffer[head]&0xFF;
                reader.pointer.value = (head+1)&bufferMask;
                notifyWriter();
                return ret;
            } else {
                if (writer.closed.value && reader.pointer.value == writer.pointer.value) return -1;
                waitForInput();
            }
        }
    }

    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        while (true) {

            final int head = reader.pointer.value;
            final int tail = getTailFromCache(head);
            if (head != tail) {
                final int ret;
                if (head < tail) {
                    ret = Math.min(tail-head, len);
                    System.arraycopy(buffer, head, b, off, ret);
                    reader.pointer.value += ret;
                } else {
                    final int endLength = buffer.length - head;
                    ret = Math.min(endLength+tail, len);
                    if (ret <= endLength) {
                        System.arraycopy(buffer, head, b, off, ret);
                        reader.pointer.value = (head+ret)&bufferMask;
                    } else {
                        System.arraycopy(buffer, head, b, off, endLength);
                        final int newHead = ret-endLength;
                        System.arraycopy(buffer, 0, b, off+endLength, newHead);
                        reader.pointer.value = newHead;
                    }
                }
                notifyWriter();
                return ret;
            } else {
                if (writer.closed.value && reader.pointer.value == writer.pointer.value) return -1;
                waitForInput();
            }
        }
    }

    private void waitForInput() throws IOException {
        synchronized (reader.lock) {
            reader.waiting.value = true;
            try {
                while (reader.pointer.value == writer.pointer.value) {
                    if (writer.closed.value) break;
                    try {
                        reader.lock.wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
            } finally {
                reader.waiting.value = false;
            }
        }
    }

    private void notifyWriter() {
        if (writer.waiting.value) {
            synchronized (writer.lock) {
                if (writer.waiting.value) {
                    if (((writer.pointer.value +1)&bufferMask) != reader.pointer.value || reader.closed.value) {
                        writer.lock.notify();
                        writer.waiting.value = false;
                    }
                }
            }
        }
    }

    private int getHeadFromCache(final int tail) {
        int head = reader.cache.value;
        if (((tail+1)&bufferMask) == head) {
            head = reader.pointer.value;
            reader.cache.value = head;
        }
        return head;
    }

    public void write(final int b) throws IOException {
        while (true) {
            final int tail = writer.pointer.value;
            final int head = getHeadFromCache(tail);
            if (((tail+1)&bufferMask) == head) {
                if (reader.closed.value) throw new IOException("InputStream is closed");
                waitForSpace();
            } else {
                buffer[tail] = (byte)b;
                writer.pointer.value = (tail+1)&bufferMask;
                notifyReader();
                return;
            }
        }
    }

    public void write(final byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > b.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        while (true) {
            if (len == 0) return;
            final int tail = writer.pointer.value;
            final int head = getHeadFromCache(tail);
            if (((tail+1)&bufferMask) == head) {
                if (reader.closed.value) throw new IOException("InputStream is closed");
                waitForSpace();
            } else {
                final int copyLen;
                if (tail < head) {
                    copyLen = Math.min(len, head-tail-1);
                } else if (head == 0) {
                    copyLen = Math.min(len, buffer.length - tail - 1);
                } else {
                    copyLen = Math.min(len, buffer.length - tail);
                }
                System.arraycopy(b, off, buffer, tail, copyLen);
                off += copyLen;
                len -= copyLen;
                writer.pointer.value = (tail+copyLen)&bufferMask;
                notifyReader();
            }
        }
    }

    public boolean writeNonBlocking(ByteBuffer bytes) throws IOException {
        while (true) {
            if (bytes.remaining() == 0) return true;
            final int tail = writer.pointer.value;
            final int head = getHeadFromCache(tail);
            if (((tail+1)&bufferMask) == head) {
                if (reader.closed.value) throw new IOException("InputStream is closed");
                return false;
            } else {
                final int copyLen;
                if (tail < head) {
                    copyLen = Math.min(bytes.remaining(), head-tail-1);
                } else if (head == 0) {
                    copyLen = Math.min(bytes.remaining(), buffer.length - tail - 1);
                } else {
                    copyLen = Math.min(bytes.remaining(), buffer.length - tail);
                }
                bytes.get(buffer, tail, copyLen);
                writer.pointer.value = (tail+copyLen)&bufferMask;
                notifyReader();
            }
        }
    }

    private void waitForSpace() throws IOException {
        synchronized (writer.lock) {
            writer.waiting.value = true;
            try {
                while (((writer.pointer.value+1)&bufferMask) == reader.pointer.value) {
                    if (reader.closed.value) break;
                    try {
                        writer.lock.wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
            } finally {
                writer.waiting.value = false;
            }
        }
    }

    private void notifyReader() {
        if (reader.waiting.value) {
            synchronized (reader.lock) {
                if (reader.waiting.value) {
                    if (reader.pointer.value != writer.pointer.value || writer.closed.value) {
                        reader.lock.notify();
                        reader.waiting.value = false;
                    }
                }
            }
        }
    }

    public void end() throws IOException {
        writer.closed.value = true;
        notifyReader();
    }

    public int available() throws IOException {
        final int head = reader.pointer.value;
        final int tail = getTailFromCache(head);
        return head < tail ? tail-head : buffer.length-head+tail;
    }

    public void close() throws IOException {
        reader.closed.value = true;
        notifyWriter();
    }
}
