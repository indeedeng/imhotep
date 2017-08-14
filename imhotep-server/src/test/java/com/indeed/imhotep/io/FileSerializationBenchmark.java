/*
 * Copyright (C) 2014 Indeed Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this path except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
 package com.indeed.imhotep.io;

import com.google.common.io.Files;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

/**
 * @author jsgroth
 */
public class FileSerializationBenchmark {
    private FileSerializationBenchmark() {
    }

    public static void main(final String[] args) throws IOException {
        final int[] a = new int[100000000];
        final Random rand = new Random();
        for (int i = 0; i < a.length; ++i) {
            a[i] = rand.nextInt();
        }
        time(new ObjectStreamSerializer(), a);
        time(new DataStreamSerializer(), a);
        time(new LEDataStreamSerializer(), a);
        time(new NIOLESerializer(), a);
        time(new NIOBESerializer(), a);
        time(new MMapLESerializer(), a);
        time(new MMapBESerializer(), a);
    }

    private static void time(final FileSerializer<int[]> s, final int[] a) throws IOException {
        time(s, a, new IntArrayChecker());
    }

    private static <T> void time(final FileSerializer<T> s, final T a, final EqualityChecker<T> equalsMethod) throws IOException {
        final File temp = File.createTempFile("temp", ".tmp");
        try {
            long serial = 0L;
            long deserial = 0L;
            for (int i = 0; i < 5; ++i) {
                temp.delete();
                System.gc(); System.gc(); System.gc();
                serial -= System.currentTimeMillis();
                s.serialize(a, temp.toPath());
                serial += System.currentTimeMillis();
                deserial -= System.currentTimeMillis();
                final T a2 = s.deserialize(temp.toPath());
                deserial += System.currentTimeMillis();
                if (!equalsMethod.equals(a, a2)) {
                    System.err.println(s.getClass().getSimpleName() + " doesn't work");
                }
            }
            serial /= 5;
            deserial /= 5;
            System.out.println("time for " + s.getClass().getSimpleName() + ": serialize=" + serial
                                       + ", deserialize=" + deserial + ", path size="
                                       + temp.length());
        } finally {
            temp.delete();
        }
    }

    private interface IntArraySerializer extends FileSerializer<int[]> {
    }

    private static class DataStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final BufferedOutputStream bos;
            bos = new BufferedOutputStream(java.nio.file.Files.newOutputStream(path));
            final DataOutputStream os = new DataOutputStream(bos);
            for (final int val : a) {
                os.writeInt(val);
            }
            os.close();
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final BufferedInputStream bis;
            bis = new BufferedInputStream(java.nio.file.Files.newInputStream(path));
            final DataInputStream is = new DataInputStream(bis);
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = is.readInt();
            }
            return ret;
        }
    }

    private static class LEDataStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final BufferedOutputStream bos;
            bos = new BufferedOutputStream(java.nio.file.Files.newOutputStream(path));
            final LittleEndianDataOutputStream os;
            os = new LittleEndianDataOutputStream(bos);
            for (final int val : a) {
                os.writeInt(val);
            }
            os.close();
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final BufferedInputStream bis;
            bis = new BufferedInputStream(java.nio.file.Files.newInputStream(path));
            final LittleEndianDataInputStream is;
            is = new LittleEndianDataInputStream(bis);
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = is.readInt();
            }
            return ret;
        }
    }

    private static class ObjectStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            writeObjectToFile(a, path);
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            return readObjectFromFile(path);
        }
    }

    private static void writeObjectToFile(final Object o, final Path path) throws IOException {
        try (OutputStream fos = java.nio.file.Files.newOutputStream(path)) {
            final ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos));
            oos.writeObject(o);
            oos.flush();
        }
    }

    private static int[] readObjectFromFile(final Path path) throws IOException {
        try (InputStream fis = java.nio.file.Files.newInputStream(path)) {
            final ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(fis));
            try {
                return (int[]) ois.readObject();
            } catch (final ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    private static class MMapBESerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final MappedByteBuffer buffer = Files.map(path.toFile(),
                                                FileChannel.MapMode.READ_WRITE,
                                                a.length * 4);
            buffer.order(ByteOrder.BIG_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; ++i) {
                intBuffer.put(i, a[i]);
            }
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final MappedByteBuffer buffer = Files.map(path.toFile(),
                                                FileChannel.MapMode.READ_ONLY,
                                                java.nio.file.Files.size(path));
            buffer.order(ByteOrder.BIG_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            intBuffer.get(ret);
            return ret;
        }
    }

    private static class NIOBESerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final FileChannel ch = new RandomAccessFile(path.toFile(), "rw").getChannel();
            final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.BIG_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; i += 2048) {
                intBuffer.clear();
                final int lim = Math.min(2048, a.length - i);
                for (int j = 0; j < lim; ++j) {
                    intBuffer.put(j, a[i+j]);
                }
                buffer.position(0).limit(4*lim);
                ch.write(buffer);
            }
            ch.close();
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final FileChannel ch = new RandomAccessFile(path.toFile(), "r").getChannel();
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.BIG_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < ret.length; i += 2048) {
                buffer.clear();
                final int lim = ch.read(buffer) / 4;
                intBuffer.clear();
                intBuffer.get(ret, i, lim);
            }
            ch.close();
            return ret;
        }
    }

    private static class NIOLESerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final FileChannel ch = new RandomAccessFile(path.toFile(), "rw").getChannel();
            final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; i += 2048) {
                intBuffer.clear();
                final int lim = Math.min(2048, a.length - i);
                intBuffer.put(a, i, lim);
                buffer.position(0).limit(4*lim);
                ch.write(buffer);
            }
            ch.close();
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final FileChannel ch = new RandomAccessFile(path.toFile(), "r").getChannel();
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            final ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < ret.length; i += 2048) {
                buffer.clear();
                final int lim = ch.read(buffer) / 4;
                intBuffer.clear();
                intBuffer.get(ret, i, lim);
            }
            ch.close();
            return ret;
        }
    }

    private static class MMapLESerializer implements IntArraySerializer {
        @Override
        public void serialize(final int[] a, final Path path) throws IOException {
            final MappedByteBuffer buffer = Files.map(path.toFile(),
                                                FileChannel.MapMode.READ_WRITE,
                                                a.length * 4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; ++i) {
                intBuffer.put(i, a[i]);
            }
        }

        @Override
        public int[] deserialize(final Path path) throws IOException {
            final MappedByteBuffer buffer = Files.map(path.toFile(),
                                                FileChannel.MapMode.READ_ONLY,
                                                java.nio.file.Files.size(path));
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            final IntBuffer intBuffer = buffer.asIntBuffer();
            final int[] ret = new int[(int)(java.nio.file.Files.size(path) / 4)];
            intBuffer.get(ret);
            return ret;
        }
    }

    private interface EqualityChecker<T> {
        boolean equals(T t1, T t2);
    }

    private static class IntArrayChecker implements EqualityChecker<int[]> {
        @Override
        public boolean equals(final int[] t1, final int[] t2) {
            return Arrays.equals(t1, t2);
        }
    }
}
