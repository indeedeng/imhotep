package com.indeed.imhotep.io;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.Pipe;

/**
 * @author jplaisance
 */
public final class NBCircularIOStream {
    private static final Logger log = Logger.getLogger(NBCircularIOStream.class);

    private final BufferedWritableSelectableChannel sink;
    private final InputStream in;

    public NBCircularIOStream() throws IOException {
        final Pipe pipe = Pipe.open();
        sink = new BufferedWritableSelectableChannel(new PipeSinkWritableSelectableChannel(pipe.sink()));
        final Pipe.SourceChannel source = pipe.source();
        sink.configureBlocking(false);
        source.configureBlocking(true);
        in = Channels.newInputStream(source);
    }

    public InputStream getInputStream() {
        return in;
    }

    public BufferedWritableSelectableChannel getOutputChannel() {
        return sink;
    }
}
