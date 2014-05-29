package com.indeed.flamdex;

import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;
import com.indeed.util.core.io.Closeables2;
import com.indeed.flamdex.reader.MockFlamdexReader;
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
        private Map<String, SortedMap<Integer, List<Integer>>> intFields = Maps.newHashMap();
        private Map<String, SortedMap<String, List<Integer>>> stringFields = Maps.newHashMap();
        private int numDocs = 0;
    }
}
