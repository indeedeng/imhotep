package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * @author jsgroth
 */
public class GzipCompressionOutputStream extends CompressionOutputStream {
    public GzipCompressionOutputStream(OutputStream out) throws IOException {
        super(new ResettableGZIPOutputStream(out));
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
        out.write(bytes, off, len);
    }

    @Override
    public void finish() throws IOException {
        ((ResettableGZIPOutputStream)out).finish();
    }

    @Override
    public void resetState() throws IOException {
        ((ResettableGZIPOutputStream)out).resetState();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    private static class ResettableGZIPOutputStream extends GZIPOutputStream {
        private ResettableGZIPOutputStream(OutputStream out) throws IOException {
            super(out);
        }

        private void resetState() {
            def.reset();
        }
    }
}
