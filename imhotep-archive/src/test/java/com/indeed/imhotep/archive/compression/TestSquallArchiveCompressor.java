package com.indeed.imhotep.archive.compression;

import junit.framework.TestCase;

import com.indeed.util.compress.CompressionInputStream;
import com.indeed.util.compress.CompressionOutputStream;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.indeed.imhotep.archive.compression.SquallArchiveCompressor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.GZIP;
import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.NONE;

/**
 * @author jsgroth
 */
public class TestSquallArchiveCompressor extends TestCase {
    private static final Logger log = Logger.getLogger(TestSquallArchiveCompressor.class);

    @Test
    public void testCompressors() throws IOException {
        compressorTestCase(NONE);
        compressorTestCase(GZIP);
    }

    private void compressorTestCase(SquallArchiveCompressor compressor) throws IOException {
        compressorTestCase(compressor, generateFixedData());
        compressorTestCase(compressor, generateRandomData());
    }

    private int[][] generateFixedData() {
        final int[][] data = new int[1000][1000];
        for (int i = 0; i < data.length; ++i) {
            for (int j = 0; j < data[i].length; ++j) {
                data[i][j] = i % 256;
            }
        }
        return data;
    }

    private int[][] generateRandomData() {
        final int[][] data = new int[1000][];
        final Random rand = new Random();
        for (int i = 0; i < data.length; ++i) {
            final int length = rand.nextInt(1000) + 1000;
            data[i] = new int[length];
            for (int j = 0; j < length; ++j) {
                data[i][j] = rand.nextInt(255);
            }
        }
        return data;
    }

    private void compressorTestCase(SquallArchiveCompressor compressor, int[][] data) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final int[] offsets = new int[1000];
        for (int i = 0; i < 1000; ++i) {
            offsets[i] = baos.size();
            final CompressionOutputStream os = compressor.newOutputStream(baos);
            for (int j = 0; j < data[i].length; ++j) {
                os.write(data[i][j]);
            }
            os.finish();
        }
        final byte[] bytes = baos.toByteArray();
        int numBytes = 0;
        for (final int[] array : data) {
            numBytes += array.length;
        }
        log.info(compressor.getKey() + " compressor compressed " + numBytes + " bytes to " + bytes.length + " bytes");
        final SeekableByteArrayInputStream bais = new SeekableByteArrayInputStream(bytes);
        for (int i = 0; i < 1000; ++i) {
            bais.seek(offsets[i]);
            final CompressionInputStream is = compressor.newInputStream(bais);
            for (int j = 0; j < data[i].length; ++j) {
                assertEquals(data[i][j], is.read());
            }
        }
    }

    private static class SeekableByteArrayInputStream extends ByteArrayInputStream {
        private SeekableByteArrayInputStream(byte buf[]) {
            super(buf);
        }

        public void seek(int pos) {
            this.pos = pos;
        }
    }
}
