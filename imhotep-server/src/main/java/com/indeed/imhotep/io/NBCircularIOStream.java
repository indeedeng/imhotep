/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
