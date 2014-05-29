package com.indeed.flamdex.utils;

import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.simple.SimpleFlamdexReader;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;

/**
 * @author dwahler
 */
public class DumpFlamdex {
    public static void main(String[] args) throws Exception {
        final SimpleFlamdexReader reader = SimpleFlamdexReader.open("/tmp/organicshard");
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
