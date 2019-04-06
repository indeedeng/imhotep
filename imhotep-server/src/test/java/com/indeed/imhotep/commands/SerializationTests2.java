package com.indeed.imhotep.commands;

import com.google.common.collect.Lists;
import com.indeed.flamdex.query.Query;
import com.indeed.flamdex.query.Term;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.QueryRemapRule;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.api.ImhotepCommand;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

public class SerializationTests2 {

    public static final String SESSION_ID = "RandomSessionIdString";
    public static final String RANDOM_SALT = "RandomSaltString";

    private ImhotepRemoteSession imhotepRemoteSession;
    private HostPortCommandHolder holder;
    private AtomicLong tempFileSizeHolder = new AtomicLong(100);

    private class HostPortCommandHolder implements Closeable {
        String host;
        int port;
        private final Thread thread;
        private final ServerSocket serverSocket;
        final CompletableFuture<ImhotepCommand> futureCommand;

        public HostPortCommandHolder(final String host, final int port, final Thread thread, final ServerSocket serverSocket, final CompletableFuture<ImhotepCommand> futureCommand) {
            this.host = host;
            this.port = port;
            this.thread = thread;
            this.serverSocket = serverSocket;
            this.futureCommand = futureCommand;
        }

        @Override
        public void close() throws IOException {
            thread.stop();
            serverSocket.close();
        }
    }

    private HostPortCommandHolder sendDummyResponseReceiveCommand() throws IOException {
        final ServerSocket serverSocket = new ServerSocket(0);
        final CompletableFuture<ImhotepCommand> futureCommand = new CompletableFuture<>();
        final Thread requestReceiver = new Thread(new Runnable() {
            @Override
            public void run() {
                try ( final Socket socket = serverSocket.accept() ) {
                    ImhotepProtobufShipping.sendProtobuf(ImhotepResponse.newBuilder().setNumGroups(0).build(), socket.getOutputStream());
                    futureCommand.complete(ImhotepCommand.readFromInputStream(socket.getInputStream()));
                } catch (IOException e) {
                    e.printStackTrace();
                    futureCommand.completeExceptionally(e);
                }
            }
        });
        requestReceiver.start();
        return new HostPortCommandHolder("localhost", serverSocket.getLocalPort(), requestReceiver, serverSocket, futureCommand);
    }

    @Before
    public void initialize() throws IOException {
        holder = sendDummyResponseReceiveCommand();
        imhotepRemoteSession = new ImhotepRemoteSession(holder.host, holder.port, SESSION_ID, tempFileSizeHolder);
    }

    @Test
    public void testGetGroupStats() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.getGroupStatsIterator(stats);
        Assert.assertEquals(holder.futureCommand.get(), new GetGroupStats(stats, SESSION_ID));
    }

    @Test
    public void testIntOrRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final String field = "field"; final long[] terms = new long[]{1,3};
        imhotepRemoteSession.intOrRegroup(field , terms, 1, 2, 3);
        Assert.assertEquals(holder.futureCommand.get(), new IntOrRegroup(field, terms, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testMetricFilter() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricFilter(stats, 0, 100, 1, 2, 3);
        Assert.assertEquals(holder.futureCommand.get(), new MetricFilter(stats, 0, 100, 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testMetricRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricRegroup(stats, 0, 100, 10, false);
        Assert.assertEquals(holder.futureCommand.get(), MetricRegroup.createMetricRegroup(stats, 0, 100, 10, false, SESSION_ID) );
    }

    @Test
    public void testMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final GroupMultiRemapRule[] rawRules = new GroupMultiRemapRule[] {
                new GroupMultiRemapRule(1, 10, new int[]{10}, new RegroupCondition[]{new RegroupCondition("field", false, 0, "strTerm", false)})
        };
        imhotepRemoteSession.regroup(rawRules, true);
        Assert.assertEquals(holder.futureCommand.get(), MultiRegroup.createMultiRegroupCommand(rawRules, true, SESSION_ID));
    }

    @Test
    public void testMultiRegroupMessagesSender() throws IOException {
        // This command isn't serialized on the server side
    }

    @Test
    public void testMultiRegroupMessagesIterator() throws IOException {
        // This command isn't serialized on the server side
    }


    @Test
    public void testNegateMetricFilter() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.metricFilter(stats, 0, 5, true);
        Assert.assertEquals(holder.futureCommand.get(), new NegateMetricFilter(stats, 0, 5, true,SESSION_ID));
    }

    @Test
    public void testRandomMetricMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.randomMetricMultiRegroup(stats, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6});
        Assert.assertEquals(holder.futureCommand.get(), new RandomMetricMultiRegroup(stats, "RandomSaltString", 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomMetricRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> stats = Lists.newArrayList("1");
        imhotepRemoteSession.randomMetricRegroup(stats, RANDOM_SALT, 0.3, 1, 2 ,3);
        Assert.assertEquals(holder.futureCommand.get(), new RandomMetricRegroup(stats, RANDOM_SALT, 0.3, 1, 2 ,3, SESSION_ID));
    }

    @Test
    public void testRandomMultiRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.randomMultiRegroup("field", true, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6});
        Assert.assertEquals(holder.futureCommand.get(), new RandomMultiRegroup("field", true, RANDOM_SALT, 1, new double[]{0.4, 0.8}, new int[]{3, 4, 6}, SESSION_ID));
    }

    @Test
    public void testRandomRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.randomRegroup("field", true, RANDOM_SALT, 0.3, 1, 2, 3);
        Assert.assertEquals(holder.futureCommand.get(), new RandomRegroup("field", true, RANDOM_SALT, 0.3, 1, 2, 3 , SESSION_ID));
    }

    @Test
    public void testRegexRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        imhotepRemoteSession.regexRegroup("field", ".*.*", 1, 2,3);
        Assert.assertEquals(holder.futureCommand.get(), new RegexRegroup("field", ".*.*", 1, 2, 3, SESSION_ID));
    }

    @Test
    public void testRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final GroupRemapRule[] rawRules = new GroupRemapRule[] {
                new GroupRemapRule(1, new RegroupCondition("fieldName", false, 0, "strTerm", false), 1000000, 1000000)
        };
        imhotepRemoteSession.regroup(rawRules);
        Assert.assertEquals(holder.futureCommand.get(), Regroup.createRegroup(rawRules, SESSION_ID));

    }

    @Test
    public void testQueryReampRule() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final QueryRemapRule rule = new QueryRemapRule(1, Query.newTermQuery(new Term("if2", true, 0, "a")),1, 2);
        imhotepRemoteSession.regroup(rule);
        Assert.assertEquals(holder.futureCommand.get(), new QueryRemapRuleRegroup(rule, SESSION_ID));
    }

    @Test
    public void testUncondictionalRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final UnconditionalRegroup unconditionalRegroup = new UnconditionalRegroup(new int[]{1,2,3}, new int[]{12,43,12}, true, SESSION_ID);
        imhotepRemoteSession.regroup(new int[]{1, 2, 3}, new int[]{12, 43, 12}, true);
        Assert.assertEquals(holder.futureCommand.get(), unconditionalRegroup);
    }

    @Test
    public void testStringOrRegroup() throws ImhotepOutOfMemoryException, ExecutionException, InterruptedException {
        final List<String> terms = Lists.newArrayList("1");
        imhotepRemoteSession.stringOrRegroup("field", terms.toArray(new String[0]), 1, 2, 3);
        Assert.assertEquals(holder.futureCommand.get(), new StringOrRegroup("field", terms, 1, 2, 3, SESSION_ID));
    }
}
