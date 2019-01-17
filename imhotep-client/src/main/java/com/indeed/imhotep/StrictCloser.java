package com.indeed.imhotep;

import com.google.common.io.Closer;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;

/**
 * Similar to com.google.common.io.Closer, but once it has been closed once,
 * any subsequently registered {@code Closeable} should be instantly closed.
 */
@ThreadSafe
public class StrictCloser implements Closeable {

    private static final Logger log = Logger.getLogger(StrictCloser.class);

    private final Closer closer = Closer.create();
    private final Object lock = new Object();
    private boolean closed = false;

    @Override
    public void close() throws IOException {
        final boolean needsClose;
        synchronized (lock) {
            needsClose = !closed;
            closed = true;
        }
        if (needsClose) {
            // This is safe to do because anyone calling registerOrClose()
            // after the above section will see the StrictCloser as closed
            // and not attempt to register in the closer
            closer.close();
        }
    }

    /**
     * Registers the given {@code closeable} to be closed when this {@code StrictCloser} is
     * {@linkplain #close closed}.
     *
     * If this {@code StrictCloser} is already closed, quietly close the given {@code closeable}
     *
     * @return the given {@code closeable}
     */
    public <C extends Closeable> C registerOrClose(@Nullable C closeable) {
        final boolean alreadyClosed;
        synchronized (lock) {
            alreadyClosed = closed;
            if (!closed) {
                closer.register(closeable);
            }
        }
        if (alreadyClosed && closeable != null) {
            Closeables2.closeQuietly(closeable, log);
        }
        return closeable;
    }
}
