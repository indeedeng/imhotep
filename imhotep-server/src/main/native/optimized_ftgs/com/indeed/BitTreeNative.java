package com.indeed;

import java.lang.System;

/**
 * @author jplaisance
 */
public final class BitTreeNative {
    private int iteratorIndex;
    private int size;
    private long handle;
    private int[] iter_buffer;
    private int count;
    
    static {
        System.loadLibrary("BitTreeNative");
    }

    public BitTreeNative(int size) {
        this.size = size;
        this.handle = this.native_init(size);
        this.iter_buffer = null;
    }

    public boolean next() {
        if (iter_buffer == null) {
            iter_buffer = new int[4096];
            count = dump(handle, iter_buffer);
            iteratorIndex = 0;
            if (count > 0) {
                return true;
            } else {
                return false;
            }
        }
        
        iteratorIndex ++;
        if (iteratorIndex < count) {
            return true;
        }

        if (count < iter_buffer.length) {
            /* that was the last buffer */
            return false;
        }

        count = dump(handle, iter_buffer);
        iteratorIndex = 0;
        if (count > 0) {
            return true;
        } else {
            return false;
        }
    }

    public int getValue() {
        return iter_buffer[iteratorIndex];
    }
    
    public void set(int idx) {
        this.native_set(handle, idx);
    }

    private native long native_init(int size);
    private native void native_destroy(long handle);
    private native void native_set(long handle, int index);
    private native int dump(long handle, int[] buffer);
}
