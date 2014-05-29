package com.indeed.imhotep.client;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;
import com.indeed.util.core.Pair;
import com.indeed.flamdex.query.BooleanOp;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepSession;
import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * @author dwahler
 */
public class QueryCollaborativeFilteringDemo {
    public static void main(String[] args) throws Exception {
        final String query1 = "firefighter";
        final String index = "organic";
        final DateTime startDate = new DateTime(2012, 5, 14, 0, 0);
        final DateTime endDate = new DateTime(2012, 5, 21, 0, 0);

        ImhotepClient client = new ImhotepClient("aus-imo01.indeed.net:2181", true);

        System.out.println("query: " + query1);

        long elapsedTime = -System.currentTimeMillis();

        ImhotepSession session = client.sessionBuilder(index, startDate, endDate).build();
        session.pushStat("count()");
        session.regroup(new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("q", false, 0, query1, false), 0, 1),
        });
        long[] totalStats = session.getGroupStats(0);

        ArrayList<String> ctks = new ArrayList<String>();
        List<Query> termQueries = new ArrayList<Query>();
        FTGSIterator it = session.getFTGSIterator(new String[]{}, new String[]{"ctk"});
        long[] stats = new long[2];
        while (it.nextField()) {
            if (!it.fieldName().equals("ctk")) throw new RuntimeException("unexpected field " + it.fieldName());
            if (it.fieldIsIntType()) throw new RuntimeException("int field!");
            while (it.nextTerm()) {
                if (!it.nextGroup()) continue;
                it.groupStats(stats);
                if (stats[0] == 0) continue;
                final String ctk = it.termStringVal();
                ctks.add(ctk);
                termQueries.add(Query.newTermQuery(new Term("ctk", false, 0, ctk)));
            }
        }
        System.out.printf("%d impressions by %d users\n\n", totalStats[1], ctks.size());

        session.close();

        session = client.sessionBuilder(index, startDate, endDate).build();
        session.pushStat("count()");
        session.pushStat("clicked");

        Query q = Query.newBooleanQuery(BooleanOp.OR, termQueries);
        q = Query.newBooleanQuery(BooleanOp.AND, ImmutableList.of(
                q,
                Query.newBooleanQuery(BooleanOp.NOT, ImmutableList.of(
                        Query.newTermQuery(new Term("q", false, 0, query1))
                ))
        ));

        session.regroup(new QueryRemapRule(1, q, 0, 1));

        System.out.println("top terms:\n");

        Map<String, TopK> fieldToTopK = Maps.newTreeMap();
        fieldToTopK.put("jobid", new TopK(15));
        fieldToTopK.put("onetid", new TopK(15));
        fieldToTopK.put("titleword", new TopK(100));
        fieldToTopK.put("q", new TopK(100));
        fieldToTopK.put("qwords", new TopK(15));
        fieldToTopK.put("sourceid", new TopK(15));

        it = session.getFTGSIterator(new String[]{"jobid","onetid","sourceid"}, new String[]{"q","qwords","titleword"});
        while (it.nextField()) {
            TopK top = fieldToTopK.get(it.fieldName());
            while (it.nextTerm()) {
                if (!it.nextGroup()) continue;
                if (top == null) continue;

                it.groupStats(stats);
                if (stats[0] == 0) continue;
                final String value = it.fieldIsIntType() ? Long.toString(it.termIntVal()) : it.termStringVal();
                top.add(value, stats);
            }
        }

        session.close();
        elapsedTime += System.currentTimeMillis();

        for (Map.Entry<String, TopK> entry : fieldToTopK.entrySet()) {
            System.out.println(entry.getKey() + ":");
            for (Pair<List<Long>, String> p : entry.getValue().getTop()) {
                List<Long> counts = p.getFirst();
                String value = p.getSecond();
                System.out.printf("%s\t%d\t%d\t%.2f%%\t\n", value,
                        counts.get(0), counts.get(1), counts.get(1)*100.0/counts.get(0));
            }
            System.out.println();
        }
        System.out.println("elapsed time: " + elapsedTime + " ms");
        client.close();
    }

    private static class TopK {
        private final int k;
        private static final Ordering<Pair<List<Long>,String>> ORDERING =
                Ordering.<Long>natural().lexicographical().onResultOf(
                    new Function<Pair<List<Long>, String>, Iterable<Long>>() {
                        @Override
                        public Iterable<Long> apply(Pair<List<Long>, String> input) {
                            return input.getFirst();
                        }
                    }
            );;

        public TopK(int k) {
            this.k = k;
            pq = new PriorityQueue<Pair<List<Long>, String>>(k,
                    ORDERING);
        }

        private final PriorityQueue<Pair<List<Long>, String>> pq;

        public void add(String value, long[] counts) {
            pq.add(Pair.of((List<Long>)Lists.newArrayList(Longs.asList(counts)), value));
            while (pq.size() > k) pq.poll();
        }

        public List<Pair<List<Long>,String>> getTop() {
            List<Pair<List<Long>,String>> result = Lists.newArrayList(pq);
            Collections.sort(result, ORDERING);
            Collections.reverse(result);
            return result;
        }
    }
}
