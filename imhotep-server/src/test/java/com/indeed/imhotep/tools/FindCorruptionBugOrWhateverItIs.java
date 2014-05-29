package com.indeed.imhotep.tools;

import com.indeed.flamdex.api.FlamdexReader;
import com.indeed.flamdex.api.IntTermIterator;
import com.indeed.flamdex.simple.SimpleFlamdexReader;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;

/**
 * @author jsgroth
 */
public class FindCorruptionBugOrWhateverItIs {
    public static void main(String[] args) throws IOException, NoSuchMethodException, IllegalAccessException, InstantiationException, InvocationTargetException {
        LongArrayList termsList = new LongArrayList();
        for (File dir : new File("/var/imhotep/shards/orgmodelsubset").listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory();
            }
        })) {
            System.out.println(dir.getName());
            FlamdexReader r = SimpleFlamdexReader.open(dir.getAbsolutePath());
            for (String intField : r.getIntFields()) {
                termsList.clear();                
                System.out.println(intField);
                IntTermIterator iterator = r.getIntTermIterator(intField);
                if (!iterator.next()) {
                    System.out.println("no terms in " + intField);
                    for (int i = 0; i < 15; ++i) {
                        IntTermIterator it = r.getIntTermIterator(intField);
                        it.reset(i);
                        it.close();
                    }
                    iterator.close();
                    continue;
                }

                termsList.add(0);
                long min = iterator.term();
                termsList.add(min);
                long max = min;
                while (iterator.next()) {
                    termsList.add(iterator.term());
                    max = iterator.term();
                }
                ++max;
                if (max < Integer.MAX_VALUE) {
                    termsList.add(max + 1);
                }

                System.out.println("min="+min+", max="+max+", len(terms)="+termsList.size());

                Collections.shuffle(termsList);
                for (int i = 0; i < termsList.size(); ++i) {
                    if (i % 100000 == 0) System.out.println("  "+i);
                    final long term = termsList.getLong(i);
                    iterator.reset(term);
                    iterator.next();
                }

                {
                    iterator.reset(0);
                    iterator.next();
                }

                if (max < Integer.MAX_VALUE) {
                    iterator.reset(max + 1);
                    iterator.next();
                }

                iterator.close();
            }
        }
    }
}
