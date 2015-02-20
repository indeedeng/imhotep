package com.indeed.imhotep.io;

import java.net.Socket;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileDescriptor;
import java.lang.reflect.Field;
 
/**
 * Utility Functions for Sockets
 */
public final class SocketUtils {
 
    private static Field __fd;
    static {
        try {
            __fd = FileDescriptor.class.getDeclaredField("fd");
            __fd.setAccessible(true);
        } catch (Exception ex) {
            __fd = null;
        }   
    }   
 
    /** 
     * Get Input Handle from Socket.
     */
    public static int getInputDescriptor(Socket s) {
        try {
            FileInputStream in = (FileInputStream)s.getInputStream();
            FileDescriptor fd = in.getFD();
            return __fd.getInt(fd);
        } catch (Exception e) { } 
        return -1; 
    }   
 
    /** 
     * Get Output Handle from Socket.
     */
    public static int getOutputDescriptor(Socket s) {
        try {
            FileOutputStream in = (FileOutputStream)s.getOutputStream();
            FileDescriptor fd = in.getFD();
            return __fd.getInt(fd);
        } catch (Exception e) { } 
        return -1; 
    }   
}