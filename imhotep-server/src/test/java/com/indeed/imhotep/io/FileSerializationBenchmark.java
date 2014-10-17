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
 package com.indeed.imhotep.io;

import com.google.common.io.Files;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Random;

/**
 * @author jsgroth
 */
public class FileSerializationBenchmark {
    public static void main(String[] args) throws IOException {
        int[] a = new int[100000000];
        Random rand = new Random();
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

    private static void time(FileSerializer<int[]> s, int[] a) throws IOException {
        time(s, a, new IntArrayChecker());
    }

    private static <T> void time(FileSerializer<T> s, T a, EqualityChecker<T> equalsMethod) throws IOException {
        File temp = File.createTempFile("temp", ".tmp");
        try {
            long serial = 0L;
            long deserial = 0L;
            for (int i = 0; i < 5; ++i) {
                temp.delete();
                System.gc(); System.gc(); System.gc();
                serial -= System.currentTimeMillis();
                s.serialize(a, temp);
                serial += System.currentTimeMillis();
                deserial -= System.currentTimeMillis();
                T a2 = s.deserialize(temp);
                deserial += System.currentTimeMillis();
                if (!equalsMethod.equals(a, a2)) System.err.println(s.getClass().getSimpleName() + " doesn't work");
            }
            serial /= 5;
            deserial /= 5;
            System.out.println("time for " + s.getClass().getSimpleName() + ": serialize="+serial+", deserialize="+deserial+", file size="+temp.length());
        } finally {
            temp.delete();
        }
    }

    private static interface IntArraySerializer extends FileSerializer<int[]> {
    }

    private static class DataStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            DataOutputStream os = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            for (int val : a) {
                os.writeInt(val);
            }
            os.close();
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
            int[] ret = new int[(int)(file.length() / 4)];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = is.readInt();
            }
            return ret;
        }
    }

    private static class LEDataStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            LittleEndianDataOutputStream os = new LittleEndianDataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
            for (int val : a) {
                os.writeInt(val);
            }
            os.close();
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            LittleEndianDataInputStream is = new LittleEndianDataInputStream(new BufferedInputStream(new FileInputStream(file)));
            int[] ret = new int[(int)(file.length() / 4)];
            for (int i = 0; i < ret.length; ++i) {
                ret[i] = is.readInt();
            }
            return ret;
        }
    }

    private static class ObjectStreamSerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            writeObjectToFile(a, file);
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            return readObjectFromFile(file);
        }
    }

    private static void writeObjectToFile(Object o, File file) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        try {
            ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(fos));
            oos.writeObject(o);
            oos.flush();
        } finally {
            fos.close();
        }
    }

    private static int[] readObjectFromFile(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        try {
            ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(fis));
            try {
                return (int[])ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        } finally {
            fis.close();
        }
    }

    private static class MMapBESerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, a.length * 4);
            buffer.order(ByteOrder.BIG_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; ++i) {
                intBuffer.put(i, a[i]);
            }
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_ONLY, file.length());
            buffer.order(ByteOrder.BIG_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            int[] ret = new int[(int)(file.length() / 4)];
            intBuffer.get(ret);
            return ret;
        }
    }

    private static class NIOBESerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            FileChannel ch = new RandomAccessFile(file, "rw").getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.BIG_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; i += 2048) {
                intBuffer.clear();
                int lim = Math.min(2048, a.length - i);
                for (int j = 0; j < lim; ++j) {
                    intBuffer.put(j, a[i+j]);
                }
                buffer.position(0).limit(4*lim);
                ch.write(buffer);
            }
            ch.close();
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            FileChannel ch = new RandomAccessFile(file, "r").getChannel();
            int[] ret = new int[(int)(file.length() / 4)];
            ByteBuffer buffer = ByteBuffer.allocateDirect(8192);
            buffer.order(ByteOrder.BIG_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < ret.length; i += 2048) {
                buffer.clear();
                int lim = ch.read(buffer) / 4;
                intBuffer.clear();
                intBuffer.get(ret, i, lim);
            }
            ch.close();
            return ret;
        }
    }

    private static class NIOLESerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            FileChannel ch = new RandomAccessFile(file, "rw").getChannel();
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
            ch.close();
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            FileChannel ch = new RandomAccessFile(file, "r").getChannel();
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
            ch.close();
            return ret;
        }
    }

    private static class MMapLESerializer implements IntArraySerializer {
        @Override
        public void serialize(int[] a, File file) throws IOException {
            MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_WRITE, a.length * 4);
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            for (int i = 0; i < a.length; ++i) {
                intBuffer.put(i, a[i]);
            }
        }

        @Override
        public int[] deserialize(File file) throws IOException {
            MappedByteBuffer buffer = Files.map(file, FileChannel.MapMode.READ_ONLY, file.length());
            buffer.order(ByteOrder.LITTLE_ENDIAN);
            IntBuffer intBuffer = buffer.asIntBuffer();
            int[] ret = new int[(int)(file.length() / 4)];
            intBuffer.get(ret);
            return ret;
        }
    }

    private static interface EqualityChecker<T> {
        boolean equals(T t1, T t2);
    }

    private static class IntArrayChecker implements EqualityChecker<int[]> {
        @Override
        public boolean equals(int[] t1, int[] t2) {
            return Arrays.equals(t1, t2);
        }
    }
}
