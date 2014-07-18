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

    ClosingInputStreamFTGSIterator(@Nullable Socket socket, InputStream is, OutputStream os, int numStats) throws IOException {
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
                } catch (IOException e) {
                    log.error(e);
                }
                try {
                    is.close();
                } catch (IOException e) {
                    log.error(e);
                }
                try {
                    if (socket != null) {
                        socket.close();
                    }
                } catch (IOException e) {
                    log.error(e);
                }
                closed = true;
            }
            return false;
        }
        return true;
    }
}
