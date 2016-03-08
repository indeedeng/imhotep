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
 package com.indeed.flamdex.writer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 */
public final class FlamdexDocument {
    @Nonnull
    private final Map<String, LongList> intFields;
    @Nonnull
    private final Map<String, List<String>> stringFields;

    public FlamdexDocument() {
        this(new HashMap<String, LongList>(), new HashMap<String, List<String>>());
    }

    public FlamdexDocument(@Nonnull final Map<String, LongList> intFields, @Nonnull final Map<String, List<String>> stringFields) {
        this.intFields = intFields;
        this.stringFields = stringFields;
    }

    @Nonnull
    public Map<String, LongList> getIntFields() {
        return intFields;
    }

    @Nonnull
    public Map<String, List<String>> getStringFields() {
        return stringFields;
    }

    public void clearIntField(@Nonnull final String field) {
        intFields.remove(field);
    }

    public void clearStringField(@Nonnull final String field) {
        stringFields.remove(field);
    }

    /**
     * Returns a list of the int terms for a given field.
     * If this field hasn't been added, null is returned.
     */
    @Nullable
    public LongList getIntTerms(@Nonnull final String field) {
        return intFields.get(field);
    }

    /**
     * Returns a list of the string terms for a given field.
     * If this field hasn't been added, null is returned.
     */
    @Nullable
    public List<String> getStringTerms(@Nonnull final String field) {
        return stringFields.get(field);
    }



    // Integer Field Setters

    private LongList prepareIntField(final String field) {
        Preconditions.checkNotNull(field, "field cannot be null");

        LongList list = intFields.get(field);
        if (list == null) {
            list = new LongArrayList();
            intFields.put(field, list);
        }
        return list;
    }

    public void setIntField(@Nonnull final String field, @Nonnull final long[] terms) {
        clearIntField(field);
        addIntTerms(field, terms);
    }
    public void addIntTerms(@Nonnull final String field, @Nonnull final long[] terms) {
        final LongList list = prepareIntField(field);
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        for (long term : terms) {
            list.add(term);
        }
    }

    public void setIntField(@Nonnull final String field, final long term) {
        clearIntField(field);
        addIntTerm(field, term);
    }
    public void addIntTerm(@Nonnull final String field, final long term) {
        prepareIntField(field).add(term);
    }

    public void setIntField(@Nonnull final String field, final boolean b) {
        clearIntField(field);
        addIntTerm(field, b);
    }
    public void addIntTerm(@Nonnull final String field, final boolean b) {
        prepareIntField(field).add(b ? 1 : 0);
    }

    public void setIntField(@Nonnull final String field, @Nonnull final Iterable<Long> terms) {
        clearIntField(field);
        addIntTerms(field, terms);
    }
    public void addIntTerms(@Nonnull final String field, @Nonnull final Iterable<Long> terms) {
        final LongList list = prepareIntField(field);
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        for (Long term : terms) {
            Preconditions.checkNotNull(term, "null terms not allowed");
            list.add(term);
        }
    }


    // String Field Setters

    private List<String> prepareStringField(final String field) {
        Preconditions.checkNotNull(field, "field cannot be null");

        List<String> list = stringFields.get(field);
        if (list == null) {
            list = new ArrayList<>();
            stringFields.put(field, list);
        }
        return list;
    }

    public void setStringField(@Nonnull final String field, @Nonnull final CharSequence term) {
        clearStringField(field);
        addStringTerm(field, term);
    }
    public void addStringTerm(@Nonnull final String field, @Nonnull final CharSequence term) {
        Preconditions.checkNotNull(term, "term cannot be null");
        prepareStringField(field).add(term.toString());
    }

    public void setStringField(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms) {
        clearStringField(field);
        addStringTerms(field, terms);
    }
    public void addStringTerms(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms) {
        final List<String> list = prepareStringField(field);
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        for (CharSequence term : terms) {
            Preconditions.checkNotNull(term, "null terms not allowed");
            list.add(term.toString());
        }
    }

    public void setStringField(@Nonnull final String field, @Nonnull final CharSequence[] terms) {
        clearStringField(field);
        addStringTerms(field, terms);
    }
    public void addStringTerms(@Nonnull final String field, @Nonnull final CharSequence[] terms) {
        final List<String> list = prepareStringField(field);
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        for (CharSequence term : terms) {
            Preconditions.checkNotNull(term, "null terms not allowed");
            list.add(term.toString());
        }
    }

    public FlamdexDocument copy() {
        final Map<String, LongList> intFieldsCopy = new HashMap<>(2*intFields.size());
        for (final Map.Entry<String, LongList> e : intFields.entrySet()) {
            intFieldsCopy.put(e.getKey(), new LongArrayList(e.getValue()));
        }
        final Map<String, List<String>> stringFieldsCopy = new HashMap<>(2*stringFields.size());
        for (final Map.Entry<String, List<String>> e : stringFields.entrySet()) {
            stringFieldsCopy.put(e.getKey(), Lists.newArrayList(e.getValue()));
        }
        return new FlamdexDocument(intFieldsCopy, stringFieldsCopy);
    }

    public static class Builder {
        private final FlamdexDocument document;
        public Builder() {
            document = new FlamdexDocument();
        }

        public Builder(final FlamdexDocument that) {
            document = that;
        }

        public Builder addStringTerm(final String field, final CharSequence term) {
            document.addStringTerm(field, term);
            return this;
        }

        public Builder addStringTerms(final String field, final CharSequence... terms) {
            document.addStringTerms(field, terms);
            return this;
        }

        public Builder addStringTerms(final String field, final Iterable<? extends CharSequence> terms) {
            document.addStringTerms(field, terms);
            return this;
        }

        public Builder addIntTerm(final String field, final boolean term) {
            document.addIntTerm(field, term);
            return this;
        }

        public Builder addIntTerm(final String field, final long term) {
            document.addIntTerm(field, term);
            return this;
        }

        public Builder addIntTerms(final String field, final long... terms) {
            document.addIntTerms(field, terms);
            return this;
        }

        public Builder addIntTerms(final String field, final Iterable<Long> terms) {
            document.addIntTerms(field, terms);
            return this;
        }

        public FlamdexDocument build() {
            return document;
        }
    }
}
