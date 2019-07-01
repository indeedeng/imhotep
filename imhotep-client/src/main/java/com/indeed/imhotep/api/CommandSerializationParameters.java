package com.indeed.imhotep.api;

import com.indeed.imhotep.client.Host;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Interface to hold RemoteSession fields for sending command and reading response
 */
public interface CommandSerializationParameters {

    String getHost();

    int getPort();

    default Host getHostAndPort() {
        return new Host(getHost(), getPort());
    }

    AtomicLong getTempFileSizeBytesLeft();
}
