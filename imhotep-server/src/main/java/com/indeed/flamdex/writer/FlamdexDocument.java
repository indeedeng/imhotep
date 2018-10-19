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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
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

    public static final int STRING_TERM_LENGTH_LIMIT = 8092;

    public FlamdexDocument() {
        this(new HashMap<>(), new HashMap<>());
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

    private LongList prepareIntField(
            final String field,
            final int capacityIfAbsent) {
        Preconditions.checkNotNull(field, "field cannot be null");

        LongList list = intFields.get(field);
        if (list == null) {
            list = new LongArrayList(capacityIfAbsent);
            intFields.put(field, list);
        }
        return list;
    }

    public void setIntField(@Nonnull final String field, @Nonnull final long[] terms) {
        clearIntField(field);
        addIntTerms(field, terms);
    }

    public void addIntTerms(@Nonnull final String field, @Nonnull final long[] terms) {
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        final LongList list = prepareIntField(field, terms.length);
        list.addElements(list.size(), terms);
    }

    public void setIntField(@Nonnull final String field, final long term) {
        clearIntField(field);
        addIntTerm(field, term);
    }

    public void addIntTerm(@Nonnull final String field, final long term) {
        prepareIntField(field, 1).add(term);
    }

    public void setIntField(@Nonnull final String field, final boolean b) {
        clearIntField(field);
        addIntTerm(field, b);
    }

    public void addIntTerm(@Nonnull final String field, final boolean b) {
        prepareIntField(field, 1).add(b ? 1 : 0);
    }

    public void setIntField(@Nonnull final String field, @Nonnull final Iterable<Long> terms) {
        setIntField(field, terms, LongArrayList.DEFAULT_INITIAL_CAPACITY);
    }

    public void addIntTerms(@Nonnull final String field, @Nonnull final Iterable<Long> terms) {
        addIntTerms(field, terms, LongArrayList.DEFAULT_INITIAL_CAPACITY);
    }

    public void setIntField(@Nonnull final String field, @Nonnull final Collection<Long> terms) {
        setIntField(field, terms, terms.size());
    }

    public void addIntTerms(@Nonnull final String field, @Nonnull final Collection<Long> terms) {
        addIntTerms(field, terms, terms.size());
    }

    // termsCount might differ from exact size of terms, it's just a hint
    public void setIntField(@Nonnull final String field, @Nonnull final Iterable<Long> terms, final int termsCount) {
        clearIntField(field);
        addIntTerms(field, terms, termsCount);
    }

    // termsCount might differ from exact size of terms, it's just a hint
    public void addIntTerms(@Nonnull final String field, @Nonnull final Iterable<Long> terms, final int termsCount) {
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        final LongList list = prepareIntField(field, termsCount);
        for (final Long term : terms) {
            Preconditions.checkNotNull(term, "null terms not allowed");
            list.add(term);
        }
    }

    // String Field Setters

    private List<String> prepareStringField(
            final String field,
            final int capacityIfAbsent) {
        Preconditions.checkNotNull(field, "field cannot be null");

        List<String> list = stringFields.get(field);
        if (list == null) {
            list = new ArrayList<>(capacityIfAbsent);
            stringFields.put(field, list);
        }
        return list;
    }

    private void checkStringTerm(final String field, final CharSequence term) {
        Preconditions.checkNotNull(term, "null terms not allowed");
        if (term.length() > STRING_TERM_LENGTH_LIMIT) {
            throw new IllegalArgumentException("Can't add a term string longer than the limit " + term.length() +
                    " > " + STRING_TERM_LENGTH_LIMIT + " in field " + field + ": " + term);
        }
    }

    public void setStringField(@Nonnull final String field, @Nonnull final CharSequence term) {
        clearStringField(field);
        addStringTerm(field, term);
    }

    public void addStringTerm(@Nonnull final String field, @Nonnull final CharSequence term) {
        checkStringTerm(field, term);
        prepareStringField(field, 1).add(term.toString());
    }

    public void setStringField(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms) {
        setStringField(field, terms, LongArrayList.DEFAULT_INITIAL_CAPACITY);
    }

    public void addStringTerms(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms) {
        addStringTerms(field, terms, LongArrayList.DEFAULT_INITIAL_CAPACITY);
    }

    public void setStringField(@Nonnull final String field, @Nonnull final Collection<? extends CharSequence> terms) {
        setStringField(field, terms, terms.size());
    }

    public void addStringTerms(@Nonnull final String field, @Nonnull final Collection<? extends CharSequence> terms) {
        addStringTerms(field, terms, terms.size());
    }

    // termsCount might differ from exact size of terms, it's just a hint
    public void setStringField(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms, final int termsCount) {
        clearStringField(field);
        addStringTerms(field, terms, termsCount);
    }

    // termsCount might differ from exact size of terms, it's just a hint
    public void addStringTerms(@Nonnull final String field, @Nonnull final Iterable<? extends CharSequence> terms, final int termsCount) {
        final List<String> list = prepareStringField(field, termsCount);
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        for (final CharSequence term : terms) {
            checkStringTerm(field, term);
            list.add(term.toString());
        }
    }

    public void setStringField(@Nonnull final String field, @Nonnull final CharSequence[] terms) {
        clearStringField(field);
        addStringTerms(field, terms);
    }

    public void addStringTerms(@Nonnull final String field, @Nonnull final CharSequence[] terms) {
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        final List<String> list = prepareStringField(field, terms.length);
        for (final CharSequence term : terms) {
            checkStringTerm(field, term);
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
