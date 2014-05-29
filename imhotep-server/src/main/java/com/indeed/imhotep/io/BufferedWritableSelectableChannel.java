package com.indeed.imhotep.io;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.IllegalBlockingModeException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;

/**
 * @author jplaisance
 */
public final class BufferedWritableSelectableChannel extends WritableSelectableChannel {
    private static final Logger log = Logger.getLogger(BufferedWritableSelectableChannel.class);

    private final ByteBuffer buffer = ByteBuffer.allocate(16384);
    private final WritableSelectableChannel wrapped;
    private boolean closed = false;

    public BufferedWritableSelectableChannel(final WritableSelectableChannel wrapped) {
        this.wrapped = wrapped;
    }

    public SelectorProvider provider() {
        return wrapped.provider();
    }

    public int validOps() {
        return wrapped.validOps();
    }

    public boolean isRegistered() {
        return wrapped.isRegistered();
    }

    public SelectionKey keyFor(final Selector sel) {
        return wrapped.keyFor(sel);
    }

    public SelectionKey register(final Selector sel, final int ops, final Object att) throws ClosedChannelException {
        return wrapped.register(sel, ops, att);
    }

    public SelectableChannel configureBlocking(final boolean block) throws IOException {
        return wrapped.configureBlocking(block);
    }

    public boolean isBlocking() {
        return wrapped.isBlocking();
    }

    public Object blockingLock() {
        return wrapped.blockingLock();
    }

    public int write(final ByteBuffer src) throws IOException {
        final int copy = Math.min(buffer.remaining(), src.remaining());
        final int limit = src.limit();
        src.limit(src.position()+copy);
        buffer.put(src);
        src.limit(limit);
        if (!buffer.hasRemaining()) {
            final int written = flushInternal();
            if (written > 0 && src.hasRemaining()) {
                return copy+write(src);
            }
        }
        return copy;
    }

    private int flushInternal() throws IOException {
        buffer.flip();
        final int written = wrapped.write(buffer);
        buffer.compact();
        return written;
    }

    public boolean flush() throws IOException {
        flushInternal();
        return buffer.position() == 0;
    }

    protected void implCloseChannel() throws IOException {
        if (!closed) {
            closed = true;
            try {
                if (buffer.hasRemaining()) {
                    wrapped.configureBlocking(true);
                    while (buffer.position() != 0) {
                        flushInternal();
                    }
                }
            } catch (IllegalBlockingModeException e) {
                // this happens when close is called asynchronously or something
                // it's not a problem because we just want everything to die
            } catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw new IOException(e);
            } finally {
                wrapped.close();
            }
        }
    }
}
