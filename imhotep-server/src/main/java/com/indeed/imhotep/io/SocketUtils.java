/*
 * Copyright (C) 2018 Indeed Inc.
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

import sun.misc.Unsafe;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.lang.reflect.Field;
import java.net.Socket;
 
/**
 * Utility Functions for Sockets
 */
public final class SocketUtils {
    private SocketUtils() {
    }

    private static final long fdOffset;
    private static final Unsafe unsafe;

    static {
        Unsafe unsafe1;
        long fdOffset1;

        try {
            final Field f1 = Unsafe.class.getDeclaredField("theUnsafe");
            f1.setAccessible(true);
            unsafe1 = (Unsafe) f1.get(null);

            final Field f2 = FileDescriptor.class.getDeclaredField("fd");
            fdOffset1 = unsafe1.objectFieldOffset(f2);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            fdOffset1 = 0;
            unsafe1 = null;
        }
        unsafe = unsafe1;
        fdOffset = fdOffset1;
    }

    /**
     * Get Output Handle from Socket.
     */
    public static int getOutputDescriptor(final Socket s) {
        try {
            final FileOutputStream in = (FileOutputStream)s.getOutputStream();
            final FileDescriptor fd = in.getFD();
            return unsafe.getInt(fd, fdOffset);
        } catch (final Exception e) {
            return -1;
        }
    }   
}