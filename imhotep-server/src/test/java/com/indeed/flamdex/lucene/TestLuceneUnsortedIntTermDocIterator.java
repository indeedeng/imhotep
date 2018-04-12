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
 package com.indeed.flamdex.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.RAMDirectory;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author jsgroth
 */
public class TestLuceneUnsortedIntTermDocIterator {
    @Test
    public void testSingleTerm() throws IOException {
        final RAMDirectory d = new RAMDirectory();
        final IndexWriter w = new IndexWriter(d, null, true, IndexWriter.MaxFieldLength.LIMITED);
        final Document doc = new Document();
        doc.add(new Field("int", "1", Field.Store.NO, Field.Index.NOT_ANALYZED_NO_NORMS));
        w.addDocument(doc);
        w.close();

        final IndexReader r = IndexReader.open(d);
        final LuceneUnsortedIntTermDocIterator iter = LuceneUnsortedIntTermDocIterator.create(r, "int");
        assertTrue(iter.nextTerm());
        assertEquals(1, iter.term());
        final int[] docs = new int[2];
        assertEquals(1, iter.nextDocs(docs));
        assertEquals(0, docs[0]);
        assertFalse(iter.nextTerm());
        r.close();
    }
}
