package com.indeed.imhotep.io;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.Pipe;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;

/**
 * @author jplaisance
 */
public final class PipeSinkWritableSelectableChannel extends GatheringSelectableChannel {
    private static final Logger log = Logger.getLogger(PipeSinkWritableSelectableChannel.class);
    private final Pipe.SinkChannel wrapped;

    public PipeSinkWritableSelectableChannel(Pipe.SinkChannel wrapped) {
        this.wrapped = wrapped;
    }

    public long write(final ByteBuffer[] srcs) throws IOException {
        return wrapped.write(srcs);
    }

    public long write(final ByteBuffer[] srcs, final int offset, final int length) throws IOException {
        return wrapped.write(srcs, offset, length);
    }

    public int write(final ByteBuffer src) throws IOException {
        return wrapped.write(src);
    }

    protected void implCloseChannel() throws IOException {
        wrapped.close();
    }

    public SelectableChannel configureBlocking(final boolean block) throws IOException {
        return wrapped.configureBlocking(block);
    }

    public Object blockingLock() {
        return wrapped.blockingLock();
    }

    public boolean isBlocking() {
        return wrapped.isBlocking();
    }

    public SelectionKey register(final Selector sel, final int ops, final Object att) throws ClosedChannelException {
        return wrapped.register(sel, ops, att);
    }

    public SelectionKey keyFor(final Selector sel) {
        return wrapped.keyFor(sel);
    }

    public boolean isRegistered() {
        return wrapped.isRegistered();
    }

    public SelectorProvider provider() {
        return wrapped.provider();
    }

    public int validOps() {
        return wrapped.validOps();
    }
}
