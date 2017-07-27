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
package com.indeed.imhotep.archive.compression;

import com.indeed.util.compress.CompressionInputStream;
import com.indeed.util.compress.CompressionOutputStream;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.GZIP;
import static com.indeed.imhotep.archive.compression.SquallArchiveCompressor.NONE;
import static org.junit.Assert.assertEquals;

/**
 * @author jsgroth
 */
public class TestSquallArchiveCompressor {
    private static final Logger log = Logger.getLogger(TestSquallArchiveCompressor.class);

    @Test
    public void testCompressors() throws IOException {
        compressorTestCase(NONE);
        compressorTestCase(GZIP);
    }

    private void compressorTestCase(final SquallArchiveCompressor compressor) throws IOException {
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

    private void compressorTestCase(final SquallArchiveCompressor compressor, final int[][] data) throws IOException {
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
        private SeekableByteArrayInputStream(final byte[] buf) {
            super(buf);
        }

        public void seek(final int pos) {
            this.pos = pos;
        }
    }
}
