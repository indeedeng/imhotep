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
 package com.indeed.flamdex;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.indeed.flamdex.reader.MockFlamdexReader;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.introspector.BeanAccess;

import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

/**
 * @author jsgroth
 *
 * see test/resources/test.yml for an example yaml flamdex
 * this class does not enforce rigorous type safety since it is expected to only be used in test code
 */
public class YamlFlamdexParser {
    private static final Logger LOG = Logger.getLogger(YamlFlamdexParser.class);

    public static MockFlamdexReader parseFromClasspathResource(final String resource) throws IOException {
        return parseFromYaml(Resources.newReaderSupplier(Resources.getResource(resource), Charsets.UTF_8));
    }

    public static MockFlamdexReader parseFromYaml(final InputSupplier<? extends Reader> inputSupplier) throws IOException {
        final Reader reader = inputSupplier.getInput();
        final YamlFlamdexReader yfr;
        try {
            final Yaml yaml = new Yaml();
            yaml.setBeanAccess(BeanAccess.FIELD);
            yfr = yaml.loadAs(reader, YamlFlamdexReader.class);
        } finally {
            Closeables2.closeQuietly(reader, LOG);
        }

        final MockFlamdexReader r = new MockFlamdexReader(yfr.intFields.keySet(), yfr.stringFields.keySet(), yfr.intFields.keySet(), yfr.numDocs);
        for (final String field : yfr.intFields.keySet()) {
            for (final Map.Entry<Integer, List<Integer>> e : yfr.intFields.get(field).entrySet()) {
                r.addIntTerm(field, e.getKey(), e.getValue());
            }
        }
        for (final String field : yfr.stringFields.keySet()) {
            for (final Map.Entry<String, List<Integer>> e : yfr.stringFields.get(field).entrySet()) {
                r.addStringTerm(field, e.getKey(), e.getValue());
            }
        }
        return r;
    }

    private static class YamlFlamdexReader {
        private final Map<String, SortedMap<Integer, List<Integer>>> intFields = Maps.newHashMap();
        private final Map<String, SortedMap<String, List<Integer>>> stringFields = Maps.newHashMap();
        private int numDocs = 0;
    }
}
