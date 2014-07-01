package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jsgroth
 */
public class NoCompressionOutputStream extends CompressionOutputStream {
    public NoCompressionOutputStream(OutputStream out) {
        super(out);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        out.write(bytes, off, len);
    }

    @Override
    public void finish() throws IOException {
        // no-op
    }

    @Override
    public void resetState() throws IOException {
        // no-op
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }
}
