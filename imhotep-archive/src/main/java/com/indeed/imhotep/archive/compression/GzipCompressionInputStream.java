package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 * @author jsgroth
 */
public class GzipCompressionInputStream extends CompressionInputStream {
    public GzipCompressionInputStream(InputStream in) throws IOException {
        super(new ResettableGZIPInputStream(in));
    }

    @Override
    public int read(byte[] bytes, int off, int len) throws IOException {
        return in.read(bytes, off, len);
    }

    @Override
    public void resetState() throws IOException {
        ((ResettableGZIPInputStream)in).resetState();
    }

    @Override
    public int read() throws IOException {
        return in.read();
    }

    private static class ResettableGZIPInputStream extends GZIPInputStream {
        private ResettableGZIPInputStream(InputStream in) throws IOException {
            super(in);
        }

        private void resetState() {
            inf.reset();
        }
    }
}
