package com.indeed.imhotep.client;

import com.indeed.imhotep.api.FTGSIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;
import java.util.StringTokenizer;

/**
 * @author jsgroth
 */
public class InteractiveShell {
    public static void main(String[] args) throws ImhotepOutOfMemoryException, IOException {
        final HostsReloader reloader;
        if (args[0].equals("--hostsfile")) {
            reloader = new FileHostsReloader(args[1]);
        } else if (args[0].equals("--zknodes")) {
            reloader = new ZkHostsReloader(args[1], true);
        } else {
            throw new RuntimeException();
        }

        main(reloader);
    }

    public static void main(final HostsReloader reloader) throws ImhotepOutOfMemoryException, IOException {
        final ImhotepClient client = new ImhotepClient(reloader);
        final Scanner sc = new Scanner(System.in);
        ImhotepSession activeSession = null;
        int numStats = 0;
        try {
            while (true) {
                System.out.print("> ");
                final String input = sc.nextLine();
                if (input.startsWith("open")) {
                    final StringTokenizer tokenizer = new StringTokenizer(input);
                    tokenizer.nextToken(); // open
                    final String dataset = tokenizer.nextToken();
                    final List<String> requestedShards = new ArrayList<String>();
                    while (tokenizer.hasMoreTokens()) {
                        requestedShards.add(tokenizer.nextToken());
                    }
                    activeSession = client.sessionBuilder(dataset, null, null).shardsOverride(requestedShards).build();
                } else if (input.equals("close")) {
                    if (activeSession != null) activeSession.close();
                    activeSession = null;
                } else if (input.startsWith("push")) {
                    final String metric = input.split(" ", 2)[1];
                    if (activeSession != null) numStats = activeSession.pushStat(metric);
                } else if (input.equals("pop")) {
                    if (activeSession != null) numStats = activeSession.popStat();
                } else if (input.startsWith("top")) {
                    final StringTokenizer tokenizer = new StringTokenizer(input);
                    tokenizer.nextToken(); // top
                    final int k = Integer.parseInt(tokenizer.nextToken());
                    String[] intFields = null;
                    String[] stringFields = null;
                    while (tokenizer.hasMoreTokens()) {
                        final String token = tokenizer.nextToken();
                        if (token.startsWith("if=")) {
                            intFields = token.substring("if=".length()).split(",");
                        } else if (token.startsWith("sf=")) {
                            stringFields = token.substring("sf=".length()).split(",");
                        }
                    }
                    if (intFields == null) intFields = new String[0];
                    if (stringFields == null) stringFields = new String[0];
                    final FTGSIterator iterator = activeSession.getFTGSIterator(intFields, stringFields);
                    final List<TGSTuple>[] topk = collectTopK(iterator, k, numStats);
                    for (int i = 0; i < numStats; ++i) {
                        System.out.println("stat "+i+":");
                        for (final TGSTuple t : topk[i]) {
                            System.out.println(t);
                        }
                    }
                } else if (input.equals("quit")) {
                    break;
                }
            }
        } finally {
            if (activeSession != null) activeSession.close();
            client.close();
        }
    }

    @SuppressWarnings("unchecked")
    public static List<TGSTuple>[] collectTopK(final FTGSIterator iterator, final int k, final int numStats) {
        final long[] statAccumBuf = new long[numStats];
        final long[] statBuf = new long[numStats];
        final PriorityQueue<TGSTuple>[] pqs = new PriorityQueue[numStats];
        for (int i = 0; i < pqs.length; ++i) {
            pqs[i] = new PriorityQueue<TGSTuple>(k+1);
        }

        while (iterator.nextField()) {
            final String fieldName = iterator.fieldName();
            final boolean fieldIsIntType = iterator.fieldIsIntType();
            while (iterator.nextTerm()) {
                final long termIntVal = fieldIsIntType ? iterator.termIntVal() : 0;
                final String termStringVal = fieldIsIntType ? null : iterator.termStringVal();

                Arrays.fill(statAccumBuf, 0L);
                while (iterator.nextGroup()) {
                    iterator.groupStats(statBuf);
                    for (int i = 0; i < numStats; ++i) {
                        statAccumBuf[i] += statBuf[i];
                    }
                }

                for (int i = 0; i < numStats; ++i) {
                    final PriorityQueue<TGSTuple> pq = pqs[i];
                    pq.add(new TGSTuple(fieldName, fieldIsIntType, termIntVal, termStringVal, statAccumBuf[i]));
                    while (pq.size() > k) pq.remove();
                }
            }
        }

        final List<TGSTuple>[] ret = new List[numStats];
        for (int i = 0; i < numStats; ++i) {
            ret[i] = new ArrayList<TGSTuple>(pqs[i]);
            Collections.sort(ret[i]);
            Collections.reverse(ret[i]);
        }
        return ret;
    }

    public static class TGSTuple implements Comparable<TGSTuple> {
        public final String field;
        public final boolean fieldIsIntType;
        public final long termIntVal;
        public final String termStringVal;
        public final long stat;

        public TGSTuple(String field, boolean fieldIsIntType, long termIntVal, String termStringVal, long stat) {
            this.field = field;
            this.fieldIsIntType = fieldIsIntType;
            this.termIntVal = termIntVal;
            this.termStringVal = termStringVal;
            this.stat = stat;
        }

        @Override
        public int compareTo(TGSTuple o) {
            return (int)(stat - o.stat);
        }

        @Override
        public String toString() {
            return field+":"+(fieldIsIntType ? termIntVal : termStringVal)+" - "+stat;
        }
    }
}
