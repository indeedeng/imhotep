package com.indeed.imhotep.local;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.indeed.flamdex.api.IntTermDocIterator;
import com.indeed.flamdex.api.StringTermDocIterator;
import com.indeed.flamdex.writer.FlamdexWriter;
import com.indeed.flamdex.writer.IntFieldWriter;
import com.indeed.flamdex.writer.StringFieldWriter;
import com.indeed.imhotep.MemoryReservationContext;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;

public class IndexReWriter {
    private final List<ImhotepLocalSession> sessions;
    private final ImhotepLocalSession newSession;
    private int[] sessionDocIdOffsets;
    private final MemoryReservationContext memory;
    private GroupLookup newGroupLookup;
    private List<int[]> perSessionMappings;
    private Map<String, DynamicMetric> dynamicMetrics;
    private long newMaxDocs;

    public IndexReWriter(List<ImhotepLocalSession> localSessions, 
                         ImhotepLocalSession newSession,
                         MemoryReservationContext memory) throws ImhotepOutOfMemoryException {
        this.sessions = localSessions;
        this.newSession = newSession;
        this.memory = memory;
        this.sessionDocIdOffsets = new int[localSessions.size()];
    }

    public GroupLookup getNewGroupLookup() {
        return this.newGroupLookup;
    }

    public Map<String, DynamicMetric> getDynamicMetrics() {
        return this.dynamicMetrics;
    }

    public List<int[]> getPerSessionMappings() {
        return perSessionMappings;
    }

    public int getNumSessionsMerged() {
        return sessions.size();
    }

    public void optimizeIndecies(@Nonnull final List<String> intFields,
                                 @Nonnull final List<String> stringFields,
                                 @Nonnull final FlamdexWriter w) throws IOException,
                                                                ImhotepOutOfMemoryException {
        final int[] docIdBuffer = new int[128];
        List<IntTermDocIterator> intIters = new ArrayList<IntTermDocIterator>(sessions.size());
        List<StringTermDocIterator> stringIters =
                new ArrayList<StringTermDocIterator>(sessions.size());
        List<Integer> sessionOffsets = new ArrayList<Integer>(this.sessionDocIdOffsets.length);
        int[] oldToNewDocIdMapping;

        oldToNewDocIdMapping = remapDocIds(this.sessions);
        w.resetMaxDocs(this.newMaxDocs);

        for (final String intField : intFields) {
            intIters.clear();
            sessionOffsets.clear();
            for (int i = 0; i < sessions.size(); i++) {
                ImhotepLocalSession session = sessions.get(i);
                IntTermDocIterator iter = session.getReader().getIntTermDocIterator(intField);
                if (iter == null) {
                    continue;
                }
                intIters.add(iter);
                sessionOffsets.add(this.sessionDocIdOffsets[i]);
            }
            final MergingIntTermDocIterator iter =
                    new MergingIntTermDocIterator(intIters, oldToNewDocIdMapping, sessionOffsets);
            final IntFieldWriter ifw = w.getIntFieldWriter(intField);
            while (iter.nextTerm()) {
                ifw.nextTerm(iter.term());
                /*
                 * Write all the terms and groups to the new index, skipping
                 * those in group 0
                 */
                int n;
                do {
                    n = iter.fillDocIdBuffer(docIdBuffer);
                    for (int i = 0; i < n; ++i) {
                        final int docId = docIdBuffer[i];
                        if (docId == -1) {
                            /* doc was in group 0 */
                            continue;
                        }
                        ifw.nextDoc(docId);
                    }
                } while (n == docIdBuffer.length);
            }
            iter.close();
            ifw.close();
        }

        for (final String stringField : stringFields) {
            stringIters.clear();
            sessionOffsets.clear();
            for (int i = 0; i < sessions.size(); i++) {
                ImhotepLocalSession session = sessions.get(i);
                StringTermDocIterator iter =
                        session.getReader().getStringTermDocIterator(stringField);
                if (iter == null) {
                    continue;
                }
                stringIters.add(iter);
                sessionOffsets.add(this.sessionDocIdOffsets[i]);
            }
            final MergingStringTermDocIterator iter =
                    new MergingStringTermDocIterator(stringIters, oldToNewDocIdMapping,
                                                     sessionOffsets);
            final StringFieldWriter sfw = w.getStringFieldWriter(stringField);
            while (iter.nextTerm()) {
                sfw.nextTerm(iter.term());
                /*
                 * Write all the terms and groups to the new index, skipping
                 * those in group 0
                 */
                int n;
                do {
                    n = iter.fillDocIdBuffer(docIdBuffer);
                    for (int i = 0; i < n; ++i) {
                        final int docId = docIdBuffer[i];
                        if (docId == -1) {
                            /* doc was in group 0 */
                            continue;
                        }
                        sfw.nextDoc(docId);
                    }
                } while (n == docIdBuffer.length);
            }
            iter.close();
            sfw.close();
        }

        this.perSessionMappings = constructPerSessionNewToOldIdMappings(oldToNewDocIdMapping);

        memory.releaseMemory(oldToNewDocIdMapping.length * 4L);
    }

    /*
     * Converts the oldToNewDocIdMapping mapping into a set of 
     * per session newToOldId mappings.  Needed for reconstructing 
     * the DynamicMetrics after one or more optimize calls followed
     * by a reset.
     * 
     * Kinda overkill now that multiple shards are not being merged 
     * anymore 
     */
    private List<int[]> constructPerSessionNewToOldIdMappings(int[] oldToNewDocIdMapping) throws ImhotepOutOfMemoryException {
        List<int[]> results = new ArrayList<int[]>(this.sessions.size());

        for (int i = 0; i < this.sessions.size(); i++) {
            int offset = this.sessionDocIdOffsets[i];
            int nDocs = this.sessions.get(i).getNumDocs();
            if (!memory.claimMemory(nDocs * 4L))
                throw new ImhotepOutOfMemoryException();
            int[] mapping = new int[nDocs];
            int last = -1;
            for (int oldDocId = offset; oldDocId < nDocs; oldDocId++) {
                int newDocId = oldToNewDocIdMapping[oldDocId];
                if (newDocId != -1) {
                    mapping[newDocId] = oldDocId;
                    last = newDocId;
                }
            }
            /* claim memory for new array that is inserted into results */
            if (!memory.claimMemory((last + 1) * 4L))
                throw new ImhotepOutOfMemoryException();
            results.add(Arrays.copyOf(mapping, last + 1));
            
            /* release memory for mapping[] */
            memory.releaseMemory(nDocs * 4L);
        }
        return results;
    }

    /*
     * Maps the existing doc ids in the sessions to a new 
     * non-overlapping set, skipping the docs in group 0.
     * 
     * Also constructs a new GroupLookup with these new doc 
     * ids, and a new DynamicMetric
     * 
     * @returns A mapping from old doc id to new doc id - with 
     *          -1 as the doc id for docs to be removed (the ones
     *          in group 0)
     */
    private int[] remapDocIds(List<ImhotepLocalSession> sessions) throws ImhotepOutOfMemoryException {
        int nTotalDocs = 0;
        int numGroups = 0;
        int newNumDocs = 0;
        int nextDocId = 0;

        /* calculate the number of docs and non-group0 docs */
        for (int i = 0; i < sessions.size(); i++) {
            this.sessionDocIdOffsets[i] = nTotalDocs;
            final ImhotepLocalSession session = sessions.get(i);
            final GroupLookup gl = session.docIdToGroup;
            final int numDocs = gl.size();
            final int grp0Docs = session.groupDocCount[0];
            nTotalDocs += numDocs;
            newNumDocs += numDocs - grp0Docs;
            numGroups = Math.max(numGroups, gl.getNumGroups());
        }

        this.newMaxDocs = newNumDocs;

        /* allocate the old doc id to new doc id mapping */
        if (!memory.claimMemory(nTotalDocs * 4L)) {
            throw new ImhotepOutOfMemoryException();
        }
        final int[] mapping = new int[nTotalDocs];

        /* populate mapping and new GroupLookup */
        GroupLookup newGL = GroupLookupFactory.create(numGroups, 
                                                      newNumDocs, 
                                                      this.newSession, 
                                                      memory);
        for (int i = 0; i < sessions.size(); i++) {
            final GroupLookup gl = sessions.get(i).docIdToGroup;
            final int offset = this.sessionDocIdOffsets[i];
            for (int j = 0; j < gl.size(); j++) {
                final int group = gl.get(j);
                if (group != 0) {
                    mapping[j + offset] = nextDocId;
                    newGL.set(nextDocId, group);
                    ++nextDocId;
                } else {
                    mapping[j + offset] = -1;
                }
            }
        }
        newGL.recalculateNumGroups();
        this.newGroupLookup = newGL;

        /*
         * remap the dynamic metrics
         */

        /* allocate the new DynamicMetrics */
        /* all session have the same # of dynamic metrics */
        int nDynMetrics = sessions.get(0).getDynamicMetrics().size();
        if (!memory.claimMemory((nTotalDocs * 4L) * nDynMetrics)) {
            throw new ImhotepOutOfMemoryException();
        }
        final Map<String, DynamicMetric> newDynMetrics = Maps.newHashMap();
        for (int i = 0; i < sessions.size(); i++) {
            ImhotepLocalSession s = sessions.get(i);
            final GroupLookup gl = s.docIdToGroup;
            final int offset = this.sessionDocIdOffsets[i];
            for (Map.Entry<String, DynamicMetric> e : s.getDynamicMetrics().entrySet()) {
                DynamicMetric oldDM = e.getValue();
                DynamicMetric newDM = newDynMetrics.get(e.getKey());
                if (newDM == null) {
                    newDM = new DynamicMetric(newNumDocs);
                }
                for (int j = 0; j < gl.size(); j++) {
                    int docId = mapping[j + offset];
                    if (docId == -1)
                        continue;
                    newDM.add(docId, oldDM.lookupSingleVal(j));
                }
                newDynMetrics.put(e.getKey(), newDM);
            }
        }
        this.dynamicMetrics = newDynMetrics;

        return mapping;
    }

}
