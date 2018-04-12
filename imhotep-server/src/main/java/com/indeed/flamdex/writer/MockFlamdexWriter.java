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
 package com.indeed.flamdex.writer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MockFlamdexWriter implements FlamdexWriter {
    private final Collection<String> intFields;
    private final Collection<String> stringFields;
    private int numDocs;

    private final Map<String, Map<Long, List<Integer>>> intTerms = new HashMap<>();
    private final Map<String, Map<String, List<Integer>>> stringTerms = new HashMap<>();

    public MockFlamdexWriter() {
        this.intFields = new ArrayList<>();
        this.stringFields = new ArrayList<>();
        this.numDocs = 0;
    }
    
    private class MockIntFieldWriter implements IntFieldWriter {
        private List<Integer> currentDocList;
        private final Map<Long, List<Integer>> terms;
        private long currentTerm;
        
        public MockIntFieldWriter(final Map<Long, List<Integer>> terms) {
            this.terms = terms;
            this.currentTerm = Integer.MIN_VALUE;
        }

        @Override
        public void nextTerm(final long term) throws IOException {
            if (this.currentTerm >= term) {
                throw new RuntimeException("Terms not inserted in order");
            }
            this.currentTerm = term;
            this.currentDocList = new ArrayList<>();
            this.terms.put(term, this.currentDocList);
        }

        @Override
        public void nextDoc(final int doc) throws IOException {
            numDocs++;
            if (!this.currentDocList.isEmpty() &&
                    doc < this.currentDocList.get(this.currentDocList.size() - 1)) {
                throw new RuntimeException("Docs not inserted in order");
            }
            this.currentDocList.add(doc);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
        
    }


    private class MockStringFieldWriter implements StringFieldWriter {
        private List<Integer> currentDocList;
        private final Map<String, List<Integer>> terms;
        private String currentTerm;
        
        public MockStringFieldWriter(final Map<String, List<Integer>> terms) {
            this.terms = terms;
            this.currentTerm = null;
        }

        @Override
        public void nextTerm(final String term) throws IOException {
            if (currentTerm != null && currentTerm.compareTo(term) >= 0) {
                throw new RuntimeException("Terms not inserted in order");
            }
            this.currentTerm = term;
            this.currentDocList = new ArrayList<>();
            this.terms.put(term, this.currentDocList);
        }

        @Override
        public void nextDoc(final int doc) throws IOException {
            numDocs++;
            if (!this.currentDocList.isEmpty() &&
                    doc < this.currentDocList.get(this.currentDocList.size() - 1)) {
                throw new RuntimeException("Docs not inserted in order");
            }
            this.currentDocList.add(doc);
        }

        @Override
        public void close() throws IOException {
            // do nothing
        }
        
    }


    @Override
    public IntFieldWriter getIntFieldWriter(final String field) throws IOException {
        this.intFields.add(field);
        this.intTerms.put(field, new LinkedHashMap<Long, List<Integer>>());
        return new MockIntFieldWriter(this.intTerms.get(field));
    }

    @Override
    public StringFieldWriter getStringFieldWriter(final String field) throws IOException {
        this.stringFields.add(field);
        this.stringTerms.put(field, new LinkedHashMap<String, List<Integer>>());
        return new MockStringFieldWriter(this.stringTerms.get(field));
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public Path getOutputDirectory() {
        return null;
    }

    public Collection<String> getIntFields() {
        return intFields;
    }

    public Collection<String> getStringFields() {
        return stringFields;
    }

    @Override
    public void resetMaxDocs(final long maxDocs) {
        /* does nothing */
    }

    public int getNumDocs() {
        return numDocs;
    }

    public Map<String, Map<Long, List<Integer>>> getIntTerms() {
        return intTerms;
    }

    public Map<String, Map<String, List<Integer>>> getStringTerms() {
        return stringTerms;
    }

}
