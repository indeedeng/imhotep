package com.indeed.imhotep.io;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class BlockInputStreamTest {
    private byte[] bytes;

    private static final int TEST_BLOCK_SIZE = 4096;
    private static final int TOTAL_DATA_SIZE = TEST_BLOCK_SIZE * 4;

    @Before
    public void setup() throws IOException {
        bytes = generateTestBytes(TOTAL_DATA_SIZE);
    }

    @Test
    public void read() throws IOException {
        test(bytes, (is, bytesRead)-> {
            int i = 0;
            while (i < bytesRead.length) {
                final byte b = (byte) is.read();
                if (b == -1) {
                    throw new EOFException();
                }
                bytesRead[i++] = b;
            }
        });
    }

    @Test
    public void readBytes() throws IOException {
        test(bytes, (is, bytesRead)-> {
            is.read(bytesRead);
        });
    }

    @Test
    public void readBytesWithOffset() throws IOException {
        test(bytes, (is, bytesRead)-> {
            final Random random = new Random(0);
            int offset = 0;
            while (offset < bytesRead.length) {
                final int len = Math.min(bytesRead.length - offset, random.nextInt(100) + 100);
                is.read(bytesRead, offset, len);
                offset += len;
            }
            is.read(bytesRead);
        });
    }

    @Test
    public void skip() throws IOException {
        test(bytes, (is, bytesRead)-> {
            is.skip(0);
            is.read(bytesRead);
        });

        test(Arrays.copyOfRange(bytes, 17, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            is.skip(17);
            is.read(bytesRead);
        });

        test(Arrays.copyOfRange(bytes, 256, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            is.skip(256);
            is.read(bytesRead);
        });

        test(Arrays.copyOfRange(bytes, 1314, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            is.skip(1314);
            is.read(bytesRead);
        });

        test(Arrays.copyOfRange(bytes, 4095, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            is.skip(4095);
            is.read(bytesRead);
        });

        test(new byte[0], (is, bytesRead)-> {
            is.skip(TOTAL_DATA_SIZE);
            is.read(bytesRead);
        });
    }

    @Test
    public void available() {

    }

    private void test(final byte[] expected, final BlockInputStreamRead func) throws IOException {
        for (final WriteMethod writeMethod : ImmutableList.of(WriteMethod.WRITE_BYTE)) {
        // for (final WriteMethod writeMethod : WriteMethod.values()) {
            try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
                // close the BlockOutputStream to flush and send last block byte
                try (final BlockOutputStream blockOs = new BlockOutputStream(os, TEST_BLOCK_SIZE)) {
                    // write bytes
                    writeToBlockOutputStream(writeMethod, blockOs, bytes);
                }

                // read bytes
                try (final InputStream is = getInputStreamFromOutputStream(os)) {
                    try (final BlockInputStream blockIs = new BlockInputStream(is, TEST_BLOCK_SIZE)) {
                        final byte[] bytesRead = new byte[expected.length];
                        func.read(blockIs, bytesRead);
                        assertArrayEquals(expected, bytesRead);
                    }
                }
            }
        }
    }

    private void writeToBlockOutputStream(final WriteMethod writeMethod, final BlockOutputStream os, final byte[] bytes) throws IOException {
        switch (writeMethod) {
            case WRITE_BYTE:
                write(os, bytes);
                break;
            case WRITE_BYTE_ARRAY:
                writeArray(os, bytes);
                break;
            case WRITE_BYTE_ARRAY_WITH_OFFSET:
                writeArrayWithOffset(os, bytes);
                break;
            case WRITE_BYTE_WITH_FLUSH:
                writeArrayWithFlush(os, bytes);
                break;
        }
    }

    private void write(final BlockOutputStream os, final byte[] bytes) throws IOException {
        for (final byte b : bytes) {
            os.write(b);
        }
    }

    private void writeArray(final BlockOutputStream os, final byte[] bytes) throws IOException {
        os.write(bytes);
    }

    private void writeArrayWithOffset(final BlockOutputStream os, final byte[] bytes) throws IOException {
        final Random random = new Random(Integer.MIN_VALUE);
        int offset = 0;
        while (offset < bytes.length) {
            final int len = Math.min(random.nextInt(100) + 100, bytes.length - offset);
            os.write(bytes, offset, len);
            offset += len;
        }
    }

    private void writeArrayWithFlush(final BlockOutputStream os, final byte[] bytes) throws IOException {
        final Random random = new Random(Integer.MAX_VALUE);
        int offset = 0;
        while (offset < bytes.length) {
            final int len = Math.min(random.nextInt(512), bytes.length - offset);
            os.write(bytes, offset, len);
            offset += len;
        }
    }

    private byte[] generateTestBytes(final int byteLength) {
        final byte[] bytes = new byte[byteLength];
        int i = 0;
        while (i < byteLength) {
            bytes[i] = (byte)(i % 128);
            i++;
        }
        return bytes;
    }

    private interface BlockInputStreamRead {
        void read(final BlockInputStream blockIn, final byte[] bytes) throws IOException;
    }

    private enum WriteMethod {
        WRITE_BYTE,
        WRITE_BYTE_ARRAY,
        WRITE_BYTE_ARRAY_WITH_OFFSET,
        WRITE_BYTE_WITH_FLUSH;
    }

    private static InputStream getInputStreamFromOutputStream(final ByteArrayOutputStream os) {
        final byte[] bytesRead = os.toByteArray();
        return new ByteArrayInputStream(bytesRead);
    }
}