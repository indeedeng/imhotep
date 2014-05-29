package com.indeed.imhotep.service;

import com.google.common.base.Charsets;
import com.indeed.imhotep.api.DocIterator;
import org.apache.log4j.Logger;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * @author jplaisance
 */
public final class DocOutputStreamWriter {
    private static final Logger log = Logger.getLogger(DocOutputStreamWriter.class);

    public static void writeNotThreadSafe(DocIterator docIterator, int numIntFields, int numStringFields, OutputStream os) throws IOException {
        final DataOutputStream out = new DataOutputStream(os);
        while (docIterator.next()) {
            writeDoc(docIterator, numIntFields, numStringFields, out);
        }
        out.writeByte(0);
        out.flush();
    }

    private static void writeDoc(final DocIterator docIterator, final int numIntFields, final int numStringFields, final DataOutputStream out) throws IOException {
        out.writeByte(1);
        out.writeInt(docIterator.getGroup());
        for (int i = 0; i < numIntFields; i++) {
            out.writeLong(docIterator.getInt(i));
        }
        for (int i = 0; i < numStringFields; i++) {
            final String str = docIterator.getString(i);
            final byte[] bytes = str.getBytes(Charsets.UTF_8);
            if (bytes.length < 0xFF) {
                out.writeByte(bytes.length);
            } else {
                out.writeByte(0xFF);
                out.writeInt(bytes.length);
            }
            out.write(bytes);
        }
    }

    public static void writeThreadSafe(DocIterator docIterator, int numIntFields, int numStringFields, DataOutputStream os) throws IOException {
        try {
            while (docIterator.next()) {
                synchronized (os) {
                    writeDoc(docIterator, numIntFields, numStringFields, os);
                }
            }
        } finally {
            docIterator.close();
        }
    }
}
