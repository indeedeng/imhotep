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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;

/**
 * @author jsgroth
 */
public class IntArraySerializer implements FileSerializer<int[]> {
    @Override
    public void serialize(int[] a, Path path) throws IOException {
        final FileSystemProvider provider = path.getFileSystem().provider();
        final FileChannel ch = provider.newFileChannel(path,
                                                       EnumSet.of(StandardOpenOption.READ,
                                                                  StandardOpenOption.WRITE,
                                                                  StandardOpenOption.CREATE));

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
    public int[] deserialize(Path path) throws IOException {
        final FileSystemProvider provider = path.getFileSystem().provider();

        try (FileChannel ch = provider.newFileChannel(path, EnumSet.of(StandardOpenOption.READ))) {
            int[] ret = new int[(int) (Files.size(path) / 4)];
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
        }
    }
}
