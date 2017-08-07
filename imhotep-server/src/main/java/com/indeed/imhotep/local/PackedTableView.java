/*
 * Copyright (C) 2016 Indeed Inc.
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
package com.indeed.imhotep.local;

import sun.misc.Unsafe;

/**
 * Provides a view from Java of the C packed_table_t struct, i.e. the
 * packed table getters defined in imhotep_native.h. Why not just call
 * through JNI to these functions, you ask? Because although JNI
 * invocation overhead is theoretically minuscule, in practice it
 * appears to kill our performance. So, the idea here is to grab the
 * stuff directly from Java with the help of sun.misc.Unsafe.
 */
public final class PackedTableView {

    private static final int SIZEOF_VECTOR = 16;

    /** Metadata from MultiCacheConfig. */
    private final MultiCacheConfig.StatsOrderingInfo[] ordering;

    // tableDataPtr and rowSizeBytes are set inside nativeBind()
    @SuppressWarnings("FieldMayBeFinal")
    private long tableDataPtr = 0;
    @SuppressWarnings("FieldMayBeFinal")
    private int rowSizeBytes = 0;

    /**
     * All our constructor needs to to is cache the 'data' member of the
     * packed_table_desc struct referred to by nativeShardDataPtr.
     *
     * @param nativeShardDataPtr - in C terms, a "packed_table_t *" as returned
     * by the native call packed_table_create.
     */
    public PackedTableView(final MultiCacheConfig.StatsOrderingInfo[] ordering,
                           final long nativeShardDataPtr) {
        this.ordering           = ordering;
        /*
      Metadata that we care about for the packed_table. These should all be
      initialized by {@link #nativeBind}.
     */
        nativeBind(nativeShardDataPtr);
    }

    public String toString() {
        return "tableDataPtr: " + tableDataPtr + " rowSizeBytes: " + rowSizeBytes;
    }

    /**
     * Extract the group id from a given row. Each row in a packed table is
     * prefixed with a 24-bit group id. (The remaining eight bits are stolen for
     * use as binary metrics.)
     *
     * @param row for which we want the group id
     * @return the group id for the given row
     */
    public int getGroup(final int row) {
        final long rowAddress = rowAddress(row);
        final int groupField = unsafe.getInt(rowAddress(row));
        return groupField & 0xFFFFFFF;
    }

    public void setGroup(final int row, final int group) {
        final long rowAddress = rowAddress(row);
        int groupField = unsafe.getInt(rowAddress(row));
        groupField = (groupField & 0xf0000000) | (group & 0x0fffffff);
        unsafe.putInt(rowAddress, groupField);
    }

    public int getIntCell(final int row, final int col) {
        return unsafe.getInt(intCellAddress(row, col));
    }

    public void setIntCell(final int row, final int col, final int value) {
        unsafe.putInt(intCellAddress(row, col), value);
    }

    private long rowAddress(final int row) {
        return tableDataPtr + rowSizeBytes * row;
    }

    private long intCellAddress(final int row, final int col) {
        final MultiCacheConfig.StatsOrderingInfo meta = ordering[col];
        final long rowAddress = rowAddress(row);
        return rowAddress + SIZEOF_VECTOR * meta.vectorNum + meta.offsetInVector;
    }

    private native void nativeBind(long nativeShardDataPtr);

    private static final Unsafe unsafe;
    static {
        try {
            final java.lang.reflect.Constructor<Unsafe> ctor = Unsafe.class.getDeclaredConstructor();
            ctor.setAccessible(true);
            unsafe = ctor.newInstance();
        }
        catch (final Throwable ex) {
            throw ex instanceof RuntimeException ?
                (RuntimeException) ex : new RuntimeException(ex);
        }
    }
}
