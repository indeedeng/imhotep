package com.indeed.imhotep.io;

import sun.misc.Unsafe;

import java.net.Socket;
import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.lang.reflect.Field;
 
/**
 * Utility Functions for Sockets
 */
public final class SocketUtils {
    private static final long fdOffset;
    private static final Unsafe unsafe;

    static {
        Unsafe unsafe1;
        long fdOffset1;

        try {
            Field f1 = Unsafe.class.getDeclaredField("theUnsafe");
            f1.setAccessible(true);
            unsafe1 = (Unsafe) f1.get(null);

            Field f2 = FileDescriptor.class.getDeclaredField("fd");
            fdOffset1 = unsafe1.objectFieldOffset(f2);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
            fdOffset1 = 0;
            unsafe1 = null;
        } catch (IllegalAccessException e) {
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
    public static int getOutputDescriptor(Socket s) {
        try {
            FileOutputStream in = (FileOutputStream)s.getOutputStream();
            FileDescriptor fd = in.getFD();
            return unsafe.getInt(fd, fdOffset);
        } catch (Exception e) { }
        return -1; 
    }   
}