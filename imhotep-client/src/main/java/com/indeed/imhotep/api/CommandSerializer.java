package com.indeed.imhotep.api;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to hold LocalSession fields for sending command and reading respononse
 */
public interface CommandSerializer {

    String getHost();

    int getPort();

    AtomicLong getTempFileSizeBytesLeft();
}
