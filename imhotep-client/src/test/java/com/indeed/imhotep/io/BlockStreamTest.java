package com.indeed.imhotep.io;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class BlockStreamTest {
    private static final int TEST_BLOCK_SIZE = 4096;
    private static final int TOTAL_DATA_SIZE = TEST_BLOCK_SIZE * 4;
    private byte[] bytes = generateTestBytes(TOTAL_DATA_SIZE);

    @Test
    public void testRead() throws IOException {
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
    public void testReadBytes() throws IOException {
        test(bytes, (is, bytesRead)-> assertEquals(bytes.length, is.read(bytesRead)));

        // test reading into an array with more spaces
        test(bytes, (is, bytesRead)-> {
            final byte[] largerBytesArray = new byte[bytes.length + 255];
            assertEquals(bytes.length, is.read(largerBytesArray));
            System.arraycopy(largerBytesArray, 0, bytesRead, 0, bytes.length);
        });
    }

    @Test
    public void testReadBytesWithOffset() throws IOException {
        test(bytes, (is, bytesRead)-> {
            final Random random = new Random(0);
            int offset = 0;
            while (offset < bytesRead.length) {
                final int len = Math.min(bytesRead.length - offset, random.nextInt(100) + 100);
                assertEquals(len, is.read(bytesRead, offset, len));
                offset += len;
            }
        });

        // test reading with larger length parameter
        test(bytes, (is, bytesRead)-> {
            final byte[] largerBytesArray = new byte[bytes.length + 255];
            assertEquals(bytes.length, is.read(largerBytesArray, 0, bytes.length + 255));
            System.arraycopy(largerBytesArray, 0, bytesRead, 0, bytes.length);
        });
    }

    @Test
    public void testSkip() throws IOException {
        test(bytes, (is, bytesRead)-> {
            assertEquals(0, is.skip(0));
            assertEquals(TOTAL_DATA_SIZE , is.read(bytesRead));
        });

        test(Arrays.copyOfRange(bytes, 17, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            assertEquals(17, is.skip(17));
            assertEquals(TOTAL_DATA_SIZE - 17, is.read(bytesRead));
        });

        test(Arrays.copyOfRange(bytes, 256, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            assertEquals(256, is.skip(256));
            assertEquals(TOTAL_DATA_SIZE - 256, is.read(bytesRead));
        });

        test(Arrays.copyOfRange(bytes, 1314, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            assertEquals(1314, is.skip(1314));
            assertEquals(TOTAL_DATA_SIZE - 1314, is.read(bytesRead));
        });

        test(Arrays.copyOfRange(bytes, 4095, TOTAL_DATA_SIZE), (is, bytesRead)-> {
            assertEquals(4095, is.skip(4095));
            assertEquals(TOTAL_DATA_SIZE - 4095, is.read(bytesRead));
        });

        test(new byte[0], (is, bytesRead)-> {
            assertEquals(TOTAL_DATA_SIZE, is.skip(TOTAL_DATA_SIZE));
            assertEquals(0, is.read(bytesRead));
        });
    }

    @Test
    public void testAvailable() throws IOException {
        test(bytes, (blockIs, bytesRead) -> {
            assertEquals(0, blockIs.available());
            blockIs.read(bytesRead, 0, 100);
            assertEquals(TEST_BLOCK_SIZE - 100, blockIs.available());
            blockIs.read(bytesRead, 100, TEST_BLOCK_SIZE - 100);
            assertEquals(0, blockIs.available());

            blockIs.read(bytesRead, TEST_BLOCK_SIZE, TEST_BLOCK_SIZE);
            assertEquals(0, blockIs.available());

            blockIs.read(bytesRead, TEST_BLOCK_SIZE * 2, 128);
            assertEquals(TEST_BLOCK_SIZE - 128, blockIs.available());
            blockIs.read(bytesRead, TEST_BLOCK_SIZE * 2 + 128, 256);
            assertEquals(TEST_BLOCK_SIZE - 384, blockIs.available());
            blockIs.read(bytesRead, TEST_BLOCK_SIZE * 2 + 384, TEST_BLOCK_SIZE - 384);
            assertEquals(0, blockIs.available());

            blockIs.read(bytesRead, TEST_BLOCK_SIZE * 3, TEST_BLOCK_SIZE);
        }, BlockOutputStreamWriteMethod.WRITE_BYTE_ARRAY);
    }

    private void test(final byte[] expected, final BlockInputStreamReader func) throws IOException {
        for (final BlockOutputStreamWriteMethod writeMethod : BlockOutputStreamWriteMethod.values()) {
            test(expected, func, writeMethod);
        }
    }

    private void test(final byte[] expected, final BlockInputStreamReader func, final BlockOutputStreamWriteMethod writeMethod) throws IOException {
        handleBlockStream((blockIs) -> {
            final byte[] bytesRead = new byte[expected.length];
            func.read(blockIs, bytesRead);
            assertArrayEquals(expected, bytesRead);
        }, writeMethod);
    }

    private void handleBlockStream(final BlockInputStreamHandler streamHandler, final BlockOutputStreamWriteMethod writeMethod) throws IOException {
        try (final ByteArrayOutputStream os = new ByteArrayOutputStream()) {
            // close the BlockOutputStream to flush and send last block byte
            try (final BlockOutputStream blockOs = new BlockOutputStream(os, TEST_BLOCK_SIZE)) {
                // writeByte bytes
                writeToBlockOutputStream(blockOs, bytes, writeMethod);
            }

            // read bytes
            try (final InputStream is = getInputStreamFromOutputStream(os)) {
                try (final BlockInputStream blockIs = new BlockInputStream(is, TEST_BLOCK_SIZE)) {
                    streamHandler.handleBlockInputStream(blockIs);
                }
            }
        }
    }

    private void writeToBlockOutputStream(final BlockOutputStream os, final byte[] bytes, final BlockOutputStreamWriteMethod writeMethod) throws IOException {
        switch (writeMethod) {
            case WRITE_BYTE:
                writeByte(os, bytes);
                break;
            case WRITE_BYTE_ARRAY:
                writeByteArray(os, bytes);
                break;
            case WRITE_BYTE_ARRAY_WITH_OFFSET:
                writeByteArrayWithOffset(os, bytes);
                break;
            case WRITE_BYTE_ARRAY_WITH_FLUSH:
                writeByteArrayWithFlush(os, bytes);
                break;
        }
    }

    private void writeByte(final BlockOutputStream os, final byte[] bytes) throws IOException {
        for (final byte b : bytes) {
            os.write(b);
        }
    }

    private void writeByteArray(final BlockOutputStream os, final byte[] bytes) throws IOException {
        os.write(bytes);
    }

    private void writeByteArrayWithOffset(final BlockOutputStream os, final byte[] bytes) throws IOException {
        final Random random = new Random(Integer.MIN_VALUE);
        int offset = 0;
        while (offset < bytes.length) {
            final int len = Math.min(random.nextInt(100) + 100, bytes.length - offset);
            os.write(bytes, offset, len);
            offset += len;
        }
    }

    private void writeByteArrayWithFlush(final BlockOutputStream os, final byte[] bytes) throws IOException {
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

    private interface BlockInputStreamReader {
        void read(final BlockInputStream blockIn, final byte[] bytesToRead) throws IOException;
    }

    private interface BlockInputStreamWriter {
        void write(final BlockOutputStream blockOs, final byte[] bytes) throws IOException;
    }

    private interface BlockInputStreamHandler {
        void handleBlockInputStream(final BlockInputStream blockIn) throws IOException;
    }

    private enum BlockOutputStreamWriteMethod {
        WRITE_BYTE,
        WRITE_BYTE_ARRAY,
        WRITE_BYTE_ARRAY_WITH_OFFSET,
        WRITE_BYTE_ARRAY_WITH_FLUSH
    }

    private static InputStream getInputStreamFromOutputStream(final ByteArrayOutputStream os) {
        final byte[] bytesRead = os.toByteArray();
        return new ByteArrayInputStream(bytesRead);
    }
}