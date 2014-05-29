package com.indeed.imhotep.service;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.reader.MockFlamdexReader;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.TimeoutException;

/**
 * @author jsgroth
 */
public class ImhotepDaemonRunner {
    private final String dir;
    private final String tempDir;
    private final int port;
    private final FlamdexReaderSource flamdexFactory;

    private ImhotepDaemon currentlyRunning;

    public ImhotepDaemonRunner(final String dir, final String tempDir, final int port) throws IOException,
                                                                                      TimeoutException {
        this(dir, tempDir, port, new FlamdexReaderSource() {
            @Override
            public FlamdexReader openReader(String directory) throws IOException {
                return new MockFlamdexReader();
            }
        });
    }

    public ImhotepDaemonRunner(final String dir,
                               String tempDir,
                               final int port,
                               final FlamdexReaderSource flamdexFactory) throws IOException,
                                                                        TimeoutException {
        this.dir = dir;
        this.tempDir = tempDir;
        this.port = port;
        this.flamdexFactory = flamdexFactory;        
    }

    public int getPort() {
        return port;
    }

    public void start() throws IOException, TimeoutException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown(false);
        }
        currentlyRunning =
                new ImhotepDaemon(new ServerSocket(port),
                                  new LocalImhotepServiceCore(dir, tempDir,
                                                              1024L * 1024 * 1024 * 1024, false,
                                                              flamdexFactory,
                                                              new LocalImhotepServiceConfig()),
                                  null, null, "localhost", port);
        new Thread(new Runnable() {
            @Override
            public void run() {
                currentlyRunning.run();
            }
        }).start();
        currentlyRunning.waitForStartup(10000L);
    }

    public void stop() throws IOException {
        if (currentlyRunning != null) {
            currentlyRunning.shutdown(false);
            currentlyRunning = null;
        }
    }
}
