package com.indeed.flamdex.utils;

import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import com.indeed.util.core.shell.PosixFileOperations;
import com.indeed.flamdex.simple.SimpleFlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

/**
 * @author dwahler
 */
public class RestoreFlamdex {
    public static void main(String[] args) throws Exception {
        long elapsed = -System.currentTimeMillis();

        final DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("/tmp/shard.dat")));
        final int numDocs = dis.readInt();

        final boolean permuteDocs = true;
        final int[] permutation = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            permutation[i] = i;
        }
        if (permuteDocs) {
            System.out.print("generating permutation...");
            final Random r = new Random(0);
            Collections.shuffle(Ints.asList(permutation), r);
            System.out.println("done.");
        }

        final int[] docIds = new int[numDocs];

        final String path = "/tmp/reconstructed";
        PosixFileOperations.rmrf(new File(path));
        final SimpleFlamdexWriter writer = new SimpleFlamdexWriter(path, numDocs, true, true);

        long termCount = 0;
        long uninvertedSize = 0;

        while (dis.readBoolean()) {
            final String strField = dis.readUTF();
            System.out.println("strField: " + strField);

            final StringFieldWriter strWriter = writer.getStringFieldWriter(strField, true);

            while (dis.readBoolean()) {
                final String strTerm = dis.readUTF();
                strWriter.nextTerm(strTerm);
                termCount++;

                final int utf8Length = strTerm.getBytes(Charsets.UTF_8).length;
                final int valueLength = 1 + varIntLength(utf8Length) + utf8Length; //field code + length + value

                int docIdCount = 0;

                while (dis.readBoolean()) {
                    final int doc = dis.readInt();
                    docIds[docIdCount++] = permutation[doc];
                }
                Arrays.sort(docIds, 0, docIdCount);

                for (int i = 0; i < docIdCount; i++) {
                    final int doc = docIds[i];
                    strWriter.nextDoc(doc);

                    uninvertedSize += valueLength;
                }
            }

            strWriter.close();
        }

        while (dis.readBoolean()) {
            final String intField = dis.readUTF();
            System.out.println("intField: " + intField);

            final IntFieldWriter intWriter = writer.getIntFieldWriter(intField, true);

            while (dis.readBoolean()) {
                final long intTerm = dis.readLong();
                intWriter.nextTerm(intTerm);
                termCount++;

                final int valueLength = 1 + varIntLength(intTerm); //field code + value

                int docIdCount = 0;
                while (dis.readBoolean()) {
                    final int doc = dis.readInt();
                    docIds[docIdCount++] = permutation[doc];
                }
                Arrays.sort(docIds, 0, docIdCount);

                for (int i = 0; i < docIdCount; i++) {
                    final int doc = docIds[i];
                    intWriter.nextDoc(doc);

                    uninvertedSize += valueLength;
                }
            }

            intWriter.close();
        }

        dis.close();
        writer.close();

        elapsed += System.currentTimeMillis();

        System.out.println("finished in " + elapsed + " ms");
        System.out.println("total terms: " + termCount);
        System.out.println("approx uninvertedSize: " + uninvertedSize);
    }

    private static int varIntLength(long l) {
        int length = 0;
        while (l >= (1<<7)) {
            length++;
            l >>= 7;
        }
        return length;
    }
}
