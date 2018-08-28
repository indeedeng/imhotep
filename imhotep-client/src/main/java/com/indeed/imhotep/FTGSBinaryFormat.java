package com.indeed.imhotep;

import com.google.common.base.Charsets;
import com.indeed.util.io.VIntUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Helper class to store FTGS into stream.
 *
 * FTGS format in stream ({x} means tag is stored with given constant or data coded with given encoding function):
 *      FTGS_in_stream  : (field_stream)* stream_end_tag{0x00}
 *      field_stream    : field_start (term)* field_end_tag{0x00 for int fields, 0x00 0x00 for string fields}
 *      field_start     : field_type{0x01 for int fields, 0x02 for string fields} field_name_len{VLong} field_name{utf-8 bytes}
 *      term            : (int_term | string_term) term_freq{SVLong} groupStats
 *      int_term        : delta_with_previous_int_term{VLong, special encoding if delta is 0}
 *      string_term     : delta_with_previous_string_term
 *      groupStats      : (groupStat)* groupStat_end_tag{0x00}
 *      groupStat       : delta_with_previous_group{VLong} stat[numStat]{each stat coded with SVLong}
 *      delta_with_previous_string_term  : remove_len_plus_one{VLong} add_len{VLong} data{utf-8 bytes}
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

    // Some helper methods for reading first term of a field.

    public static long readIntTerm(final InputStream stream, final long prevTerm) throws IOException {
        final long delta = VIntUtils.readVInt64(stream);
        return prevTerm + delta;
    }

    public static long readFirstIntTerm(final InputStream stream) throws IOException {
        return readIntTerm(stream, -1); // initial value is -1;
    }

    public static byte[] readFirstStringTerm(final InputStream stream) throws IOException {
        final int removeLengthPlusOne = VIntUtils.readVInt(stream);
        if (removeLengthPlusOne != 1) {
            throw new IllegalStateException();
        }
        final int addLength = VIntUtils.readVInt(stream);
        final byte[] term = new byte[addLength];
        if (stream.read(term) != addLength) {
            throw new IllegalStateException();
        }

        return term;
    }

    // info about one field of binary FTGS stream
    public static class FieldStat {
        public FieldStat(final String fieldName, final boolean isIntField) {
            this.fieldName = fieldName;
            this.isIntField = isIntField;
        }

        public boolean hasTerms() {
            return startPosition != endPosition;
        }

        public final String fieldName;
        public final boolean isIntField;

        // First and last terms. Make sense only if startPosition != endPosition
        public long firstIntTerm;
        public long lastIntTerm;
        public byte[] firstStringTerm;
        public byte[] lastStringTerm;

        // position of first byte of first term.
        public long startPosition;
        // position of first byte after last term.
        public long endPosition;
    }
}
