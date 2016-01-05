package com.indeed.imhotep.local;

import java.util.Random;

class UnusedDocsIds {
    final Random rand;
    final int[] docIds;
    int len;
    int nValidDocs;

    UnusedDocsIds(Random rand, int size) {
        this.rand = rand;
        this.docIds = new int[size];
        for (int i = 0; i < size; i++) {
            docIds[i] = i;
        }
        this.len = size;
        this.nValidDocs = size;
    }

    int size() {
        return this.nValidDocs;
    }

    int getRandId() {
        if (nValidDocs < len / 2) {
            this.compress();
        }

        while (true) {
            final int i = rand.nextInt(len);
            if (docIds[i] < 0) {
                continue;
            }
            final int docId = docIds[i];
            docIds[i] = -1;
            nValidDocs --;
            return docId;
        }
    }

    private void compress() {
        int max = 0;
        int trailing = 0;

        for (int i = 0; i < this.len; i++) {
            if (docIds[i] < 0) {
                continue;
            }
            docIds[trailing] = docIds[i];
            trailing++;
            if (max < docIds[i]) {
                max = docIds[i];
            }
        }

        this.len = this.nValidDocs;
    }
}
