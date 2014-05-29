package com.indeed.imhotep.tools;

import com.google.common.collect.ImmutableMap;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;
import com.indeed.imhotep.service.FTGSOutputStreamWriter;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author jsgroth
 */
public class MakeFTGSFiles {
    private static final String outputDir = "/home/jsgroth/ftgsmerger";

    private static final Map<String, List<String>> shardLists = ImmutableMap.<String, List<String>>builder()
            .put("aus-hdn02.indeed.net", Arrays.asList("index20110906, index20110808, index20110810, index20110704".split(", ")))
            .put("aus-hdn03.indeed.net", Arrays.asList("index20110725, index20110824, index20110722, index20110710, index20110828".split(", ")))
            .put("aus-hdn04.indeed.net", Arrays.asList("index20110720, index20110905, index20110701, index20110717, index20110716, index20110903".split(", ")))
            .put("aus-hdn05.indeed.net", Arrays.asList("index20110907, index20110904".split(", ")))
            .put("aus-hdn06.indeed.net", Arrays.asList("index20110804, index20110712, index20110822, index20110825, index20110728, index20110818, index20110724, index20110709, index20110723".split(", ")))
            .put("aus-hdn07.indeed.net", Arrays.asList("index20110711, index20110707, index20110721, index20110703, index20110715".split(", ")))
            .put("aus-hdn08.indeed.net", Arrays.asList("index20110823, index20110902, index20110731".split(", ")))
            .put("aus-hdn09.indeed.net", Arrays.asList("index20110829, index20110802, index20110830, index20110815, index20110714, index20110817, index20110812, index20110821, index20110813".split(", ")))
            .put("aus-hdn10.indeed.net", Arrays.asList("index20110719, index20110726, index20110727, index20110901, index20110708, index20110805, index20110819, index20110807, index20110827".split(", ")))
            .put("aus-hdn11.indeed.net", Arrays.asList("index20110706, index20110713, index20110809, index20110803, index20110816, index20110826, index20110729, index20110806, index20110730".split(", ")))
            .put("aus-hdn12.indeed.net", Arrays.asList("index20110718, index20110705, index20110801, index20110831, index20110811, index20110814, index20110820, index20110702".split(", ")))
            .build();

    public static void main(String[] args) throws IOException, ImhotepOutOfMemoryException {
        if (!new File(outputDir).exists() && !new File(outputDir).mkdirs()) {
            throw new IOException("could not create "+outputDir);
        }
        for (String hostname : shardLists.keySet()) {
            ImhotepSession session = ImhotepRemoteSession.openSession(hostname, 12345, "orgmodelsubset", shardLists.get(hostname), null);
            try {
                session.pushStat("impressions");
                session.pushStat("clicks");
                FTGSIterator iterator = session.getFTGSIterator(new String[]{"jobid"}, new String[]{});
                OutputStream os = new BufferedOutputStream(new FileOutputStream(new File(outputDir, hostname.substring(0, "aus-hdn01".length()))+".ftgs"), 65536);
                try {
                    FTGSOutputStreamWriter.write(iterator, 2, os);
                } finally {
                    os.close();
                }
            } finally {
                session.close();
            }
        }
    }
}
