package com.indeed.imhotep.io;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;

/**
 * @author jsgroth
 */
public class IntArraySerializer implements FileSerializer<int[]> {
    @Override
    public void serialize(int[] a, File file) throws IOException {
        FileChannel ch = new RandomAccessFile(file, "rw").getChannel();
        try {
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; i += 2048) {
                intBuffer.clear();
                int lim = Math.min(2048, a.length - i);
                intBuffer.put(a, i, lim);
                buffer.position(0).limit(4*lim);
                ch.write(buffer);
            }
        } finally {
            ch.close();
        }
    }

    @Override
    public int[] deserialize(File file) throws IOException {
        FileChannel ch = new RandomAccessFile(file, "r").getChannel();
        try {
            int[] ret = new int[(int)(file.length() / 4)];
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < ret.length; i += 2048) {
                buffer.clear();
                int lim = ch.read(buffer) / 4;
                intBuffer.clear();
                intBuffer.get(ret, i, lim);
            }
            return ret;
        } finally {
            ch.close();
        }
    }
}
