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
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
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

    public void setIntField(@Nonnull final String field, @Nonnull final LongList terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");

        intFields.put(field, terms);
    }

    public void setIntField(@Nonnull final String field, @Nonnull final long[] terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");

        intFields.put(field, new LongArrayList(terms));
    }

    public void setIntField(@Nonnull final String field, final long term) {
        Preconditions.checkNotNull(field, "field cannot be null");

        intFields.put(field, new LongArrayList(new long[]{term}));
    }

    public void setIntField(@Nonnull final String field, final boolean b) {
        Preconditions.checkNotNull(field, "field cannot be null");

        intFields.put(field, new LongArrayList(new long[]{b ? 1 : 0}));
    }

    public void setIntField(@Nonnull final String field, @Nonnull final List<Long> terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");

        intFields.put(field, new LongArrayList(terms));
    }

    public void addIntTerm(@Nonnull final String field, final long term) {
        Preconditions.checkNotNull(field, "field cannot be null");

        if (!intFields.containsKey(field)) {
            intFields.put(field, new LongArrayList());
        }
        intFields.get(field).add(term);
    }

    public void addIntTerms(@Nonnull final String field, @Nonnull final Collection<Long> terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        Preconditions.checkArgument(Iterables.all(terms, Predicates.notNull()), "null terms not allowed");

        if (!intFields.containsKey(field)) {
            intFields.put(field, new LongArrayList());
        }
        intFields.get(field).addAll(terms);
    }

    public void setStringField(@Nonnull final String field, @Nonnull final List<String> terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        Preconditions.checkArgument(Iterables.all(terms, Predicates.notNull()), "null terms not allowed");

        stringFields.put(field, terms);
    }

    public void setStringField(@Nonnull final String field, @Nonnull final String[] terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");

        final List<String> termsList = Arrays.asList(terms);
        Preconditions.checkArgument(Iterables.all(termsList, Predicates.notNull()), "null terms not allowed");

        stringFields.put(field, termsList);
    }

    public void setStringField(@Nonnull final String field, @Nonnull final String term) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(term, "term cannot be null");

        stringFields.put(field, Arrays.asList(term));
    }

    public void addStringTerm(@Nonnull final String field, @Nonnull final String term) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(term, "term cannot be null");

        if (!stringFields.containsKey(field)) {
            stringFields.put(field, new ArrayList<String>());
        }
        stringFields.get(field).add(term);
    }

    public void addStringTerms(@Nonnull final String field, @Nonnull final Collection<String> terms) {
        Preconditions.checkNotNull(field, "field cannot be null");
        Preconditions.checkNotNull(terms, "terms list cannot be null");
        Preconditions.checkArgument(Iterables.all(terms, Predicates.notNull()), "null terms not allowed");

        if(!stringFields.containsKey(field)) {
            stringFields.put(field, new ArrayList<String>());
        }
        stringFields.get(field).addAll(terms);
    }

    public FlamdexDocument copy() {
        final Map<String, LongList> intFieldsCopy = new HashMap<String, LongList>(2*intFields.size());
        for (final Map.Entry<String, LongList> e : intFields.entrySet()) {
            intFieldsCopy.put(e.getKey(), new LongArrayList(e.getValue()));
        }
        final Map<String, List<String>> stringFieldsCopy = new HashMap<String, List<String>>(2*stringFields.size());
        for (final Map.Entry<String, List<String>> e : stringFields.entrySet()) {
            stringFieldsCopy.put(e.getKey(), Lists.newArrayList(e.getValue()));
        }
        return new FlamdexDocument(intFieldsCopy, stringFieldsCopy);
    }
}
