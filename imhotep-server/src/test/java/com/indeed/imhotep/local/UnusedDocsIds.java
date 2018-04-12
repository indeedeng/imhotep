/*
 * Copyright (C) 2018 Indeed Inc.
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

import java.util.Random;

class UnusedDocsIds {
    private final Random rand;
    private final int[] docIds;
    private int len;
    private int nValidDocs;

    UnusedDocsIds(final Random rand, final int size) {
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
