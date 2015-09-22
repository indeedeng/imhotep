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
 package com.indeed.flamdex.lucene;

import com.indeed.flamdex.AbstractFlamdexReader;
import com.indeed.flamdex.api.DocIdStream;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.api.StringTermIterator;
import com.indeed.flamdex.fieldcache.UnsortedIntTermDocIterator;
import com.indeed.flamdex.utils.FlamdexUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexReader;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

public class LuceneFlamdexReader extends AbstractFlamdexReader {
    private static final Logger log = Logger.getLogger(LuceneFlamdexReader.class);

    protected final IndexReader reader;

    protected final Collection<String> intFields;
    protected final Collection<String> stringFields;

    public LuceneFlamdexReader(IndexReader reader) {
        this(reader, Collections.<String>emptyList(), getStringFieldsFromIndex(reader));
    }

    public LuceneFlamdexReader(IndexReader reader, String directory) {
        this(reader, directory, Collections.<String>emptyList(), getStringFieldsFromIndex(reader));
    }

    public LuceneFlamdexReader(IndexReader reader, Collection<String> intFields, Collection<String> stringFields) {
        this(reader, System.getProperty("java.io.tmpdir"), intFields, stringFields);
    }

    public LuceneFlamdexReader(IndexReader reader, String directory, Collection<String> intFields, Collection<String> stringFields) {
        super(directory, reader.maxDoc());

        this.reader = reader;
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    @Override
    public Collection<String> getIntFields() {
        return intFields;
    }

    @Override
    public Collection<String> getStringFields() {
        return stringFields;
    }

    private static Collection<String> getStringFieldsFromIndex(final IndexReader reader) {
        final Collection<String> ret = new HashSet<String>();
        // don't like having to use Object and downcast, but in Lucene versions prior to 3 getFieldNames() returns an un-genericized Collection instead of a Collection<String>
        for (final Object o : reader.getFieldNames(IndexReader.FieldOption.INDEXED)) {
            ret.add((String)o);
        }
        return ret;
    }

    @Override
    public int getNumDocs() {
        return reader.maxDoc();
    }

    @Override
    public DocIdStream getDocIdStream() {
        try {
            return new LuceneDocIdStream(reader.termDocs());
        } catch (IOException e) {
            throw LuceneUtils.ioRuntimeException(e);
        }
    }

    @Override
    public IntTermIterator getIntTermIterator(final String field) {
        return new LuceneIntTermIterator(reader, field);
    }

    @Override
    public IntTermIterator getUnsortedIntTermIterator(final String field) {
        return new LuceneUnsortedIntTermIterator(reader, field);
    }

    @Override
    public StringTermIterator getStringTermIterator(final String field) {
        return new LuceneStringTermIterator(reader, field);
    }

    @Override
    public long getIntTotalDocFreq(String field) {
        return FlamdexUtils.getIntTotalDocFreq(this, field);
    }

    @Override
    public long getStringTotalDocFreq(String field) {
        return FlamdexUtils.getStringTotalDocFreq(this, field);
    }

    @Override
    public Collection<String> getAvailableMetrics() {
        return intFields;
    }

    @Override
    protected UnsortedIntTermDocIterator createUnsortedIntTermDocIterator(String field) {
        try {
            return LuceneUnsortedIntTermDocIterator.create(reader, field);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }
}
