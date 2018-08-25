package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.indeed.util.io.VIntUtils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Helper class to store FTGS into stream.
 *
 * FTGS format in stream:
 *      FTGS_in_stream  : (field_stream)* stream_end_tag
 *      field_stream    : field_start (term)* field_end_tag
 *      field_start     : field_type field_name_len field_name
 *      term            : (int_term | string_term) term_freq groupStats
 *      int_term        : delta_with_previous_int_term
 *      string_term     : delta_with_previous_string_term
 *      groupStats      : (groupStat)* groupStat_end_tag
 *      groupStat       : delta_with_previous_group stat[numStat]
 */

public class FTGSBinaryFormat {

    private FTGSBinaryFormat() {
    }

    public static void writeFtgsEndTag(final OutputStream stream) throws IOException {
        stream.write(0);
    }

    public static void writeFieldStart(final boolean isIntType, final String field, final OutputStream stream) throws IOException {
        if (isIntType) {
            stream.write(1);
        } else {
            stream.write(2);
        }
        final byte[] fieldBytes = field.getBytes(Charsets.UTF_8);
        writeVLong(fieldBytes.length, stream);
        stream.write(fieldBytes);
    }

    public static void writeFieldEnd(final boolean isIntField, final OutputStream stream) throws IOException {
        stream.write(0);
        if (!isIntField) {
            stream.write(0);
        }
    }

    public static void writeIntTermStart(final long term, final long prevTerm, final OutputStream stream) throws IOException {
        if (term == prevTerm) {
            //still decodes to 0 but allows reader to distinguish between end of field and delta of zero
            stream.write(0x80);
            stream.write(0);
        } else {
            writeVLong(term - prevTerm, stream);
        }
    }

    public static void writeStringTermStart(
            final byte[] term,
            final int termLen,
            final byte[] prevTerm,
            final int prevTermLen,
            final OutputStream stream) throws IOException {
        final int pLen = prefixLen(prevTerm, term, Math.min(prevTermLen, termLen));
        writeVLong((prevTermLen - pLen) + 1, stream);
        writeVLong(termLen - pLen, stream);
        stream.write(term, pLen, termLen - pLen);
    }

    public static void writeTermDocFreq(final long freq, final OutputStream stream) throws IOException {
        writeSVLong(freq, stream);
    }

    public static void writeGroup(final int groupId, final int prevGroupId, final OutputStream stream) throws IOException {
        writeVLong(groupId - prevGroupId, stream);
    }

    public static void writeStat(final long stat, final OutputStream stream) throws IOException {
        writeSVLong(stat, stream);
    }

    public static void writeGroupStatsEnd(final OutputStream out) throws IOException {
        out.write(0);
    }

    private static int prefixLen(final byte[] a, final byte[] b, final int max) {
        for (int i = 0; i < max; i++) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return max;
    }

    private static void writeVLong(final long i, final OutputStream stream) throws IOException {
        VIntUtils.writeVInt64(stream, i);
    }

    private static void writeSVLong(final long i, final OutputStream stream) throws IOException {
        VIntUtils.writeSVInt64(stream, i);
    }

}
