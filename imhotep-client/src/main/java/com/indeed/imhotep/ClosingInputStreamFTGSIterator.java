/*
 * Copyright (C) 2018 Indeed Inc.
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
 package com.indeed.imhotep;

import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

/**
 * @author jsgroth
 */
final class ClosingInputStreamFTGSIterator extends InputStreamFTGSIterator {
    private static final Logger log = Logger.getLogger(ClosingInputStreamFTGSIterator.class);

    private final Socket socket;
    private final InputStream is;
    private final OutputStream os;
    private boolean closed = false;

    ClosingInputStreamFTGSIterator(
            @Nullable final Socket socket,
            final InputStream is,
            final OutputStream os,
            final int numStats) throws IOException {
        super(is, numStats);
        this.socket = socket;
        this.is = is;
        this.os = os;
    }

    @Override
    public boolean nextField() {
        if (!super.nextField()) {
            if (!closed) {
                try {
                    os.close();
                } catch (final IOException e) {
                    log.error(e);
                }
                try {
                    is.close();
                } catch (final IOException e) {
                    log.error(e);
                }
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (final IOException e) {
                    log.error(e);
                }
                closed = true;
            }
            return false;
        }
        return true;
    }
}
