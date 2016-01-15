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
 package com.indeed.flamdex.utils;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.simple.SimpleFlamdexReader;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.file.Paths;

/**
 * @author dwahler
 */
public class DumpFlamdex {
    public static void main(String[] args) throws Exception {
        final SimpleFlamdexReader reader = SimpleFlamdexReader.open(Paths.get("/tmp/organicshard"));
        final DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/shard.dat")));

        dos.writeInt(reader.getNumDocs());

        final DocIdStream stream = reader.getDocIdStream();
        final int[] docIdBuffer = new int[1024];
        int n;

        for (String strField : reader.getStringFields()) {
            System.out.println("strField: " + strField);
            dos.writeBoolean(true);
            dos.writeUTF(strField);

            final StringTermIterator iter = reader.getStringTermIterator(strField);
            while (iter.next()) {
                final String term = iter.term();
                dos.writeBoolean(true);
                dos.writeUTF(term);

                stream.reset(iter);
                do {
                    n = stream.fillDocIdBuffer(docIdBuffer);
                    for (int i = 0; i < n; i++) {
                        dos.writeBoolean(true);
                        dos.writeInt(docIdBuffer[i]);
                    }
                } while (n == docIdBuffer.length);
                dos.writeBoolean(false);
            }

            dos.writeBoolean(false);
        }
        dos.writeBoolean(false);

        for (String intField : reader.getIntFields()) {
            System.out.println("intField: " + intField);
            dos.writeBoolean(true);
            dos.writeUTF(intField);

            final IntTermIterator iter = reader.getIntTermIterator(intField);
            while (iter.next()) {
                final long term = iter.term();
                dos.writeBoolean(true);
                dos.writeLong(term);

                stream.reset(iter);
                do {
                    n = stream.fillDocIdBuffer(docIdBuffer);
                    for (int i = 0; i < n; i++) {
                        dos.writeBoolean(true);
                        dos.writeInt(docIdBuffer[i]);
                    }
                } while (n == docIdBuffer.length);
                dos.writeBoolean(false);
            }
            dos.writeBoolean(false);
        }
        dos.writeBoolean(false);

        dos.close();
    }
}
