package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author jsgroth
 */
public class NoCompressionInputStream extends CompressionInputStream {
    public NoCompressionInputStream(InputStream in) throws IOException {
        super(in);
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return in.read(bytes, off, len);
    }

    @Override
    public void resetState() throws IOException {
        // no-op
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }
}
