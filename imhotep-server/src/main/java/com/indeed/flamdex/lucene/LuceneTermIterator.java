package com.indeed.flamdex.lucene;

import org.apache.lucene.index.TermEnum;

interface LuceneTermIterator {
    public TermEnum termEnum();
}
