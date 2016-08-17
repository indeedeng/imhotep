/*
 * Copyright (C) 2016 Indeed Inc.
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
 package com.indeed.flamdex.simple;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;

/**
 * @author dwahler
 */
final class ByteChannelDocIdStream extends SimpleDocIdStream {
    private static final Logger LOG = Logger.getLogger(ByteChannelDocIdStream.class);

    private static final int BUFFER_SIZE = 8192;
    private static final EnumSet<StandardOpenOption> OPEN_OPTIONS = EnumSet.of(StandardOpenOption.READ);

    private SeekableByteChannel channel;
    private long length;

    public ByteChannelDocIdStream() {
        super(new byte[BUFFER_SIZE]);
    }

    @Override
    protected void openFile(final Path filePath) throws IOException {
        if (channel != null) channel.close();
        channel = Files.newByteChannel(filePath, OPEN_OPTIONS);
        length = channel.size();
    }

    @Override
    protected long getLength() {
        return length;
    }

    @Override
    protected void readBytes(long offset) {
        try {
            wrappedBuffer.rewind();
            channel.position(offset).read(wrappedBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            if (channel != null) {
                channel.close();
                channel = null;
            }
        } catch (IOException e) {
            LOG.error("error closing file", e);
        }
    }
}
