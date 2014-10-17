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
