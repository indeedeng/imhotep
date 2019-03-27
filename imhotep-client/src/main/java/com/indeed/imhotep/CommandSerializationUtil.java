package com.indeed.imhotep;

import com.indeed.imhotep.api.CommandSerializer;

import java.util.concurrent.atomic.AtomicLong;

public class CommandSerializationUtil implements CommandSerializer {

    private final String host;
    private final int port;
    private final AtomicLong tempFileSizeBytesLeft;


    CommandSerializationUtil(final String host, final int port, final AtomicLong tempFileSizeBytesLeft) {
        this.host = host;
        this.port = port;
        this.tempFileSizeBytesLeft = tempFileSizeBytesLeft;
    }

    @Override
    public String getHost() {
        return host;
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public AtomicLong getTempFileSizeBytesLeft() {
        return tempFileSizeBytesLeft;
    }
}
