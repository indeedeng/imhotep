package com.indeed.imhotep.io;

import com.google.common.base.Throwables;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * @author jplaisance
 */
public final class Octopus {
    private static final Logger log = Logger.getLogger(Octopus.class);

    private final CircularInputStream[] streams;
    private final CircularInputStream.PaddedInt lock = new CircularInputStream.PaddedInt();
    private final CircularInputStream.PaddedVolatileBool waiting = new CircularInputStream.PaddedVolatileBool();

    public Octopus(int numSplits, int bufferSize) {
        streams = new CircularInputStream[numSplits];

        for (int i = 0; i < streams.length; i++) {
            streams[i] = new CircularInputStream(bufferSize, new CircularInputStream.State(lock, waiting));
        }
    }

    public boolean write(ByteBuffer buffer, int index) throws IOException {
        return streams[index].writeNonBlocking(buffer);
    }

    public void broadcast(ByteBuffer[] buffers) throws IOException {
        synchronized (lock) {
            while (true) {
                boolean block = false;
                for (int i = 0; i < streams.length; i++) {
                    block |= !streams[i].writeNonBlocking(buffers[i]);
                }
                if (!block) {
                    waiting.value = false;
                    return;
                }
                if (waiting.value) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        waiting.value = false;
                        throw Throwables.propagate(e);
                    }
                } else {
                    waiting.value = true;
                }
            }
        }
    }

    public InputStream getInputStream(int index) {
        return streams[index];
    }

    public void closeOutput() throws IOException {
        Error error = null;
        for (CircularInputStream in : streams) {
            try {
                in.end();
            } catch (Error e) {
                if (error == null) {
                    error = e;
                } else {
                    log.error("supressing error", e);
                }
            } catch (Exception e) {
                log.error("error", e);
            }
        }
        if (error != null) throw error;
    }
}
