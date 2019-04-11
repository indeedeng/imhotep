package com.indeed.imhotep.api;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to hold RemoteSession fields for sending command and reading response
 */
public interface CommandSerializationParameters {

    String getHost();

    int getPort();

    AtomicLong getTempFileSizeBytesLeft();
}
