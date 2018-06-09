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
package com.indeed.imhotep.service;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.flamdex.query.Query;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepRemoteSession;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.api.PerformanceStats;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.fs.RemoteCachingFileSystemProvider;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.NioPathUtil;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.IntFieldAndTerms;
import com.indeed.imhotep.protobuf.QueryMessage;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.StringFieldAndTerms;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.rpc.RequestResponseClient;
import com.indeed.imhotep.shardmaster.rpc.RequestResponseClientFactory;
import com.indeed.util.core.Pair;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ImhotepDaemon implements Instrumentation.Provider {
    private static final Logger log = Logger.getLogger(ImhotepDaemon.class);

    private final ServerSocket ss;

    private final ExecutorService executor;

    private final AbstractImhotepServiceCore service;
    private final ServiceZooKeeperWrapper zkWrapper;

    private final AtomicLong requestIdCounter = new AtomicLong(0);

    private volatile boolean isStarted = false;

    private final ShardUpdateListener shardUpdateListener;

    private final @Nullable Integer sessionForwardingPort;

    private final InstrumentationProvider instrumentation =
        new InstrumentationProvider();

    private ServiceCoreObserver serviceCoreObserver;

    private static final class InstrumentationProvider
        extends Instrumentation.ProviderSupport {

        private final ThreadMXBean mxb     = ManagementFactory.getThreadMXBean();
        private final Runtime      runtime = Runtime.getRuntime();

        @Override
        public synchronized void fire(final Instrumentation.Event event) {
            try {
                event.getProperties().put(Keys.FREE_MEMORY, runtime.freeMemory());
                event.getProperties().put(Keys.TOTAL_MEMORY, runtime.totalMemory());
                event.getProperties().put(Keys.DAEMON_THREAD_COUNT, mxb.getDaemonThreadCount());
                event.getProperties().put(Keys.PEAK_THREAD_COUNT, mxb.getPeakThreadCount());
                event.getProperties().put(Keys.THREAD_COUNT, mxb.getThreadCount());
            }
            finally {
                super.fire(event);
            }
        }
    }

    private static final class ShardUpdateListener
        implements ShardUpdateListenerIf {

        private final AtomicReference<ImhotepResponse> datasetMetadataResponse =
            new AtomicReference<>();

        private final AtomicReference<ImhotepResponse> datasetListResponse =
            new AtomicReference<>();

        public void onDatasetUpdate(final List<DatasetInfo> datasetList,
                                    final Source unusedSource) {
            final ImhotepResponse.Builder shardListBuilder = ImhotepResponse.newBuilder();
            for (final DatasetInfo dataset : datasetList) {
                shardListBuilder.addDatasetInfo(dataset.toProto());
            }
            final ImhotepResponse shardListReponse = shardListBuilder.build();
            datasetListResponse.set(shardListReponse);


            final ImhotepResponse.Builder metadataBuilder = ImhotepResponse.newBuilder();
            for (final DatasetInfo dataset : datasetList) {
                metadataBuilder.addDatasetInfo(dataset.toProtoNoShards());
            }
            final ImhotepResponse metadataResponse = metadataBuilder.build();
            datasetMetadataResponse.set(metadataResponse);
        }

        public ImhotepResponse getDatasetMetadataResponse() { return datasetMetadataResponse.get();   }
        public ImhotepResponse getDatasetListResponse() { return datasetListResponse.get(); }
    }

    /* Relays events to our observers. */
    private final class ServiceCoreObserver implements Instrumentation.Observer {
        public void onEvent(final Instrumentation.Event event) {
            instrumentation.fire(event);
        }
    }

    public ImhotepDaemon(
            final ServerSocket ss,
            final AbstractImhotepServiceCore service,
            final String zkNodes,
            final String zkPath,
            final String hostname,
            final int port,
            final ShardUpdateListener shardUpdateListener,
            final @Nullable Integer sessionForwardingPort) {
        this.ss = ss;
        this.service = service;
        this.shardUpdateListener = shardUpdateListener;
        this.sessionForwardingPort = sessionForwardingPort;
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;
            @Override
            public Thread newThread(@Nonnull final Runnable r) {
                return new Thread(r, "ImhotepDaemonRemoteServiceThread"+i++);
            }
        });

        zkWrapper = zkNodes != null ?
            new ServiceZooKeeperWrapper(zkNodes, hostname, port, zkPath) : null;
    }

    /** Intended for tests that create their own ImhotepDaemons. */
    @VisibleForTesting
    public ImhotepDaemon(
            final ServerSocket ss,
            final AbstractImhotepServiceCore service,
            final String zkNodes,
            final String zkPath,
            final String hostname,
            final int port,
            final @Nullable Integer sessionForwardingPort) {
        this(ss, service, zkNodes, zkPath, hostname, port, new ShardUpdateListener(), sessionForwardingPort);
    }

    public void addObserver(final Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(final Instrumentation.Observer observer) {
        instrumentation.removeObserver(observer);
    }

    public void run() {
        NDC.push("main");

        try {
            log.info("starting up daemon");
            isStarted = true;
            //noinspection InfiniteLoopStatement
            while (!ss.isClosed()) {
                try {
                    final Socket socket = ss.accept();
                    socket.setSoTimeout(60000);
                    socket.setTcpNoDelay(true);
                    log.debug("received connection, running");
                    executor.execute(new DaemonWorker(socket));
                } catch (final IOException e) {
                    log.warn("server socket error", e);
                }
            }
        } finally {
            NDC.pop();
        }
    }

    public boolean isStarted() {
        return isStarted;
    }

    public void waitForStartup(final long timeout) throws TimeoutException {
        final long startTime = System.currentTimeMillis();
        while (!isStarted() && (System.currentTimeMillis() - startTime) < timeout) {}
        if (!isStarted()) {
            throw new TimeoutException("ImhotepDaemon failed to start within " + timeout + " ms");
        }
    }

    static void sendResponse(final ImhotepResponse response, final OutputStream os) throws IOException {
        log.debug("sending response");
        ImhotepProtobufShipping.sendProtobuf(response, os);
        log.debug("response sent");
    }

    private static void sendGroupStat(final GroupStatsIterator groupStats, final OutputStream os) throws IOException {
        log.debug("sending group stats");
        ImhotepProtobufShipping.writeGroupStats(groupStats, os);
        log.debug("group stats sent");
    }

    private class DaemonWorker implements Runnable {
        private final Socket socket;
        private final String localAddr;

        private DaemonWorker(final Socket socket) {
            this.socket = socket;

            String tmpAddr;
            try {
                tmpAddr = InetAddress.getLocalHost().toString();
            }
            catch (final Exception ex) {
                tmpAddr = "";
                log.warn("cannot initialize localAddr", ex);
            }
            this.localAddr = tmpAddr;
        }

        @Override
        public void run() {
            try {
                final InetAddress remoteAddress = socket.getInetAddress();
                NDC.push("DaemonWorker(" + socket.getRemoteSocketAddress() + ")");
                try {
                    internalRun();
                } finally {
                    NDC.pop();
                }
            } catch (final RuntimeException e) {
                if (e.getCause() instanceof SocketException) {
                    log.warn("worker exception", e);
                } else if (e instanceof IllegalArgumentException) {
                    log.warn("worker exception", e);
                } else {
                    log.error("worker exception", e);
                }
                throw e;
            }
        }

        private ImhotepResponse openSession(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException, IOException {

            if (ImhotepDaemon.this.sessionForwardingPort != null && request.getAllowSessionForwarding()) {
                final String host = "127.0.0.1";
                final int port = ImhotepDaemon.this.sessionForwardingPort;
                log.trace("Forwarding OPEN_SESSION request to " + host + ":" + port);

                try (Socket socket = ImhotepRemoteSession.newSocket(host, port, 0)) {
                    final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());
                    final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());

                    final List<String> shards = request.getShardRequestList();
                    log.trace("sending open request to "+host+":"+port+" for shards "+ shards);

                    ImhotepProtobufShipping.sendProtobuf(request, os);

                    log.trace("waiting for confirmation from "+host+":"+port);
                    final ImhotepResponse initialResponse = ImhotepProtobufShipping.readResponse(is);

                    final ImhotepResponse newResponse = initialResponse.toBuilder().setNewPort(port).build();
                    return newResponse;
                }
            } else {
                final InetAddress inetAddress = socket.getInetAddress();
                final AtomicLong tempFileSizeBytesLeft =
                        request.getTempFileSizeLimit() > 0 ?
                                new AtomicLong(request.getTempFileSizeLimit()) : null;

                final String sessionId =
                        service.handleOpenSession(request.getDataset(),
                                request.getShardRequestList(),
                                request.getUsername(),
                                request.getClientName(),
                                inetAddress.getHostAddress(),
                                request.getClientVersion(),
                                request.getMergeThreadLimit(),
                                request.getOptimizeGroupZeroLookups(),
                                request.getSessionId(),
                                tempFileSizeBytesLeft,
                                request.getSessionTimeout());
                NDC.push(sessionId);
                builder.setSessionId(sessionId);
                return builder.build();
            }
        }

        private ImhotepResponse closeSession(final ImhotepRequest          request,
                                             final ImhotepResponse.Builder builder) {
            final String sessionId = request.getSessionId();
            if(request.getReturnStatsOnClose()) {
                final PerformanceStats stats = service.handleCloseAndGetPerformanceStats(request.getSessionId());
                if (stats != null) {
                    builder.setPerformanceStats(ImhotepDaemonMarshaller.marshal(stats));
                }
            } else {
                service.handleCloseSession(sessionId);
            }
            return builder.build();
        }

        private ImhotepResponse regroup(final ImhotepRequest          request,
                                        final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final List<GroupRemapMessage> remapRulesList = request.getRemapRulesList();
            final GroupRemapRule[] groupRemapMessageList =
                ImhotepDaemonMarshaller.marshalGroupRemapMessageList(remapRulesList);
            final int numGroups =
                service.handleRegroup(request.getSessionId(), groupRemapMessageList);
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse explodedRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final InputStream             is)
            throws ImhotepOutOfMemoryException {
            final int numRules = request.getLength();
            final int numGroups =
                service.handleRegroup(request.getSessionId(),
                                      numRules,
                                      new UnmodifiableIterator<GroupRemapRule>() {
                                          private int i = 0;

                                          @Override
                                          public boolean hasNext() {
                                              return i < numRules;
                                          }

                                          @Override
                                          public GroupRemapRule next() {
                                              try {
                                                  final GroupRemapMessage message =
                                                  ImhotepProtobufShipping.readGroupRemapMessage(is);
                                                  final GroupRemapRule rule =
                                                  ImhotepDaemonMarshaller.marshal(message);
                                                  i++;
                                                  return rule;
                                              } catch (final IOException e) {
                                                  throw Throwables.propagate(e);
                                              }
                                          }
                                      });
            return builder.setNumGroups(numGroups).build();
        }

        private ImhotepResponse queryRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final QueryRemapMessage remapMessage = request.getQueryRemapRule();
            final int numGroups =
                service.handleQueryRegroup(request.getSessionId(),
                                           ImhotepDaemonMarshaller.marshal(remapMessage));
            return builder.setNumGroups(numGroups).build();
        }

        private ImhotepResponse intOrRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleIntOrRegroup(request.getSessionId(),
                                       request.getField(),
                                       Longs.toArray(request.getIntTermList()),
                                       request.getTargetGroup(),
                                       request.getNegativeGroup(),
                                       request.getPositiveGroup());
            return builder.build();
        }

        private ImhotepResponse stringOrRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final String[] termList = new String[request.getStringTermCount()];
            service.handleStringOrRegroup(request.getSessionId(),
                                          request.getField(),
                                          request.getStringTermList().toArray(termList),
                                          request.getTargetGroup(),
                                          request.getNegativeGroup(),
                                          request.getPositiveGroup());
            return builder.build();
        }

        private ImhotepResponse randomRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleRandomRegroup(request.getSessionId(),
                                        request.getField(),
                                        request.getIsIntField(),
                                        request.getSalt(),
                                        request.getP(),
                                        request.getTargetGroup(),
                                        request.getNegativeGroup(),
                                        request.getPositiveGroup());
            return builder.build();
        }

        private ImhotepResponse randomMultiRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleRandomMultiRegroup(request.getSessionId(),
                                             request.getField(),
                                             request.getIsIntField(),
                                             request.getSalt(),
                                             request.getTargetGroup(),
                                             Doubles.toArray(request.getPercentagesList()),
                                             Ints.toArray(request.getResultGroupsList()));
            return builder.build();
        }

        private ImhotepResponse randomMetricRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
                throws ImhotepOutOfMemoryException {
            service.handleRandomMetricRegroup(request.getSessionId(),
                    request.getStat(),
                    request.getSalt(),
                    request.getP(),
                    request.getTargetGroup(),
                    request.getNegativeGroup(),
                    request.getPositiveGroup());
            return builder.build();
        }

        private ImhotepResponse randomMetricMultiRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
                throws ImhotepOutOfMemoryException {
            service.handleRandomMetricMultiRegroup(request.getSessionId(),
                    request.getStat(),
                    request.getSalt(),
                    request.getTargetGroup(),
                    Doubles.toArray(request.getPercentagesList()),
                    Ints.toArray(request.getResultGroupsList()));
            return builder.build();
        }

        private ImhotepResponse regexRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleRegexRegroup(request.getSessionId(),
                                       request.getField(),
                                       request.getRegex(),
                                       request.getTargetGroup(),
                                       request.getNegativeGroup(),
                                       request.getPositiveGroup());
            return builder.build();
        }

        private ImhotepResponse getTotalDocFreq(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final long totalDocFreq =
                service.handleGetTotalDocFreq(request.getSessionId(),
                                              getIntFields(request),
                                              getStringFields(request));
            builder.setTotalDocFreq(totalDocFreq);
            return builder.build();
        }

        private ImhotepResponse getGroupStats(final ImhotepRequest          request,
                                              final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final GroupStatsIterator groupStats =
                    service.handleGetGroupStats(request.getSessionId(),
                            request.getStat());
            builder.setGroupStatSize(groupStats.getNumGroups());
            while (groupStats.hasNext()) {
                builder.addGroupStat(groupStats.nextLong());
            }
            return builder.build();
        }

        private Pair<ImhotepResponse, GroupStatsIterator> getStreamingGroupStats(final ImhotepRequest request,
                                                            final ImhotepResponse.Builder builder)
        {
            final GroupStatsIterator groupStats =
                service.handleGetGroupStats(request.getSessionId(),
                                            request.getStat());
            builder.setGroupStatSize(groupStats.getNumGroups());
            return Pair.of(builder.build(), groupStats);
        }

        private Pair<ImhotepResponse, GroupStatsIterator> getDistinct(final ImhotepRequest request,
                                                          final ImhotepResponse.Builder builder)
        {
            final GroupStatsIterator groupStats =
                    service.handleGetDistinct(request.getSessionId(),
                            request.getField(), request.getIsIntField());
            builder.setGroupStatSize(groupStats.getNumGroups());
            return Pair.of(builder.build(), groupStats);
        }

        private Pair<ImhotepResponse, GroupStatsIterator> mergeDistinctSplit(final ImhotepRequest request,
                                                               final ImhotepResponse.Builder builder)
        {
            final InetSocketAddress[] nodes =
                    Lists.transform(request.getNodesList(),
                            new Function<HostAndPort, InetSocketAddress>() {
                                public InetSocketAddress apply(final HostAndPort input) {
                                    return new InetSocketAddress(input.getHost(),
                                            input.getPort());
                                }
                            }).toArray(new InetSocketAddress[request.getNodesCount()]);

            final GroupStatsIterator groupStats =
                    service.handleMergeDistinctSplit(
                            request.getSessionId(),
                            request.getField(),
                            request.getIsIntField(),
                            nodes,
                            request.getSplitIndex());
            builder.setGroupStatSize(groupStats.getNumGroups());
            return Pair.of(builder.build(), groupStats);
        }

        private void getFTGSIterator(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException {
            checkSessionValidity(request);
            service.handleGetFTGSIterator(request.getSessionId(),
                                          getIntFields(request),
                                          getStringFields(request),
                                          request.getTermLimit(),
                                          request.getSortStat(),
                                          os);
        }

        private void getSubsetFTGSIterator(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetSubsetFTGSIterator(request.getSessionId(),
                                                getIntFieldsToTerms(request),
                                                getStringFieldsToTerms(request),
                                                os);
        }

        private void getFTGSSplit(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetFTGSIteratorSplit(request.getSessionId(),
                                               getIntFields(request),
                                               getStringFields(request),
                                               os,
                                               request.getSplitIndex(),
                                               request.getNumSplits(),
                                               request.getTermLimit());
        }

        private void getSubsetFTGSSplit(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetSubsetFTGSIteratorSplit(request.getSessionId(),
                                                     getIntFieldsToTerms(request),
                                                     getStringFieldsToTerms(request),
                                                     os,
                                                     request.getSplitIndex(),
                                                     request.getNumSplits());
        }

        private void mergeFTGSSplit(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException {
            checkSessionValidity(request);
            final InetSocketAddress[] nodes =
                Lists.transform(request.getNodesList(),
                                new Function<HostAndPort, InetSocketAddress>() {
                                    public InetSocketAddress apply(final HostAndPort input) {
                                        return new InetSocketAddress(input.getHost(),
                                                                     input.getPort());
                                    }
                                }).toArray(new InetSocketAddress[request.getNodesCount()]);
            service.handleMergeFTGSIteratorSplit(request.getSessionId(),
                                                 getIntFields(request),
                                                 getStringFields(request),
                                                 os, nodes, request.getSplitIndex(), request.getTermLimit(), request.getSortStat());
        }

        private void mergeSubsetFTGSSplit(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            final InetSocketAddress[] nodes =
                Lists.transform(request.getNodesList(),
                                new Function<HostAndPort, InetSocketAddress>() {
                                    public InetSocketAddress apply(final HostAndPort input) {
                                        return new InetSocketAddress(input.getHost(),
                                                                     input.getPort());
                                    }
                                }).toArray(new InetSocketAddress[request.getNodesCount()]);
            service.handleMergeSubsetFTGSIteratorSplit(request.getSessionId(),
                                                       getIntFieldsToTerms(request),
                                                       getStringFieldsToTerms(request),
                                                       os, nodes, request.getSplitIndex());
        }

        private ImhotepResponse pushStat(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numStats = service.handlePushStat(request.getSessionId(),
                                                        request.getMetric());
            builder.setNumStats(numStats);
            return builder.build();
        }

        private ImhotepResponse popStat(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numStats = service.handlePopStat(request.getSessionId());
            builder.setNumStats(numStats);
            return builder.build();
        }

        private ImhotepResponse getNumGroups(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numGroups = service.handleGetNumGroups(request.getSessionId());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse getShardInfoList(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            ImhotepResponse response = shardUpdateListener.getDatasetListResponse();
            if (response == null) {
                final List<DatasetInfo> datasets = service.handleGetDatasetList();
                for (final DatasetInfo dataset : datasets) {
                    builder.addDatasetInfo(dataset.toProto());
                }
                response = builder.build();
            }
            return response;
        }

        private ImhotepResponse getShardlistForTime(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
                throws ImhotepOutOfMemoryException {

            final List<ShardInfo> shards = service.handleGetShardlistForTime(request.getDataset(), request.getStartUnixtime(), request.getEndUnixtime());
            for (final ShardInfo shardInfo: shards) {
                builder.addShardInfo(shardInfo.toProto());
            }
            return builder.build();
        }

        private ImhotepResponse getDatasetMetadata(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
                throws ImhotepOutOfMemoryException {
            ImhotepResponse response = shardUpdateListener.getDatasetMetadataResponse();
            if (response == null) {
                final List<DatasetInfo> datasets = service.handleGetDatasetList();
                for (final DatasetInfo dataset : datasets) {
                    builder.addDatasetInfo(dataset.toProtoNoShards());
                }
                response = builder.build();
            }
            return response;
        }

        private ImhotepResponse getStatusDump(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final ImhotepStatusDump statusDump = service.handleGetStatusDump(request.getIncludeShardList());
            builder.setStatusDump(statusDump.toProto());
            return builder.build();
        }

        private ImhotepResponse metricRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numGroups = service.handleMetricRegroup(request.getSessionId(),
                                                              request.getXStat(),
                                                              request.getXMin(),
                                                              request.getXMax(),
                                                              request.getXIntervalSize(),
                                                              request.getNoGutters());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse metricRegroup2D(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numGroups = service.handleMetricRegroup2D(request.getSessionId(),
                                                                request.getXStat(),
                                                                request.getXMin(),
                                                                request.getXMax(),
                                                                request.getXIntervalSize(),
                                                                request.getYStat(),
                                                                request.getYMin(),
                                                                request.getYMax(),
                                                                request.getYIntervalSize());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse metricFilter(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numGroups = service.handleMetricFilter(request.getSessionId(),
                                                             request.getXStat(),
                                                             request.getXMin(),
                                                             request.getXMax(),
                                                             request.getNegate());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse createDynamicMetric(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleCreateDynamicMetric(request.getSessionId(),
                                              request.getDynamicMetricName());
            return builder.build();
        }

        private ImhotepResponse updateDynamicMetric(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleUpdateDynamicMetric(request.getSessionId(),
                                              request.getDynamicMetricName(),
                                              Ints.toArray(request.getDynamicMetricDeltasList()));
            return builder.build();
        }

        private ImhotepResponse conditionalUpdateDynamicMetric(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final List<RegroupConditionMessage> conditionsList = request.getConditionsList();
            final RegroupCondition[] conditions =
                ImhotepDaemonMarshaller.marshalRegroupConditionMessageList(conditionsList);
            final int[] deltas = Ints.toArray(request.getDynamicMetricDeltasList());
            service.handleConditionalUpdateDynamicMetric(request.getSessionId(),
                                                         request.getDynamicMetricName(),
                                                         conditions, deltas);
            return builder.build();
        }

        private ImhotepResponse groupConditionalUpdateDynamicMetric(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final List<RegroupConditionMessage> conditionsList = request.getConditionsList();
            final RegroupCondition[] conditions =
                ImhotepDaemonMarshaller.marshalRegroupConditionMessageList(conditionsList);
            final int[] deltas = Ints.toArray(request.getDynamicMetricDeltasList());
            service.handleGroupConditionalUpdateDynamicMetric(request.getSessionId(),
                                                              request.getDynamicMetricName(),
                                                              Ints.toArray(request.getGroupsList()),
                                                              conditions, deltas);
            return builder.build();
        }

        private ImhotepResponse groupQueryUpdateDynamicMetric(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
                throws ImhotepOutOfMemoryException {
            final Query[] queries = new Query[request.getQueryMessagesCount()];
            for (int i = 0; i < request.getQueryMessagesCount(); i++) {
                final QueryMessage queryMessage = request.getQueryMessages(i);
                queries[i] = ImhotepDaemonMarshaller.marshal(queryMessage);
            }
            final int[] deltas = Ints.toArray(request.getDynamicMetricDeltasList());
            service.handleGroupQueryUpdateDynamicMetric(request.getSessionId(),
                    request.getDynamicMetricName(),
                    Ints.toArray(request.getGroupsList()),
                    queries, deltas);
            return builder.build();
        }

        private ImhotepResponse optimizeSession(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleRebuildAndFilterIndexes(request.getSessionId(),
                                                  getIntFields(request),
                                                  getStringFields(request));
            return builder.build();
        }

        private ImhotepResponse resetGroups(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleResetGroups(request.getSessionId());
            return builder.build();
        }

        private ImhotepResponse multisplitRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final List<GroupMultiRemapMessage> ruleList = request.getMultisplitRemapRuleList();
            final GroupMultiRemapRule[] remapRules =
                ImhotepDaemonMarshaller.marshalGroupMultiRemapMessageList(ruleList);
            final int numGroups =
                service.handleMultisplitRegroup(request.getSessionId(),
                                                remapRules,
                                                request.getErrorOnCollisions());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse explodedMultisplitRegroup(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder,
                final InputStream             is)
            throws ImhotepOutOfMemoryException {
            final int numRules = request.getLength();
            final UnmodifiableIterator<GroupMultiRemapRule> it =
                new UnmodifiableIterator<GroupMultiRemapRule>() {
                  private int i = 0;

                  @Override
                  public boolean hasNext() {
                      return i < numRules;
                  }

                  @Override
                  public GroupMultiRemapRule next() {
                      try {
                          final GroupMultiRemapMessage message =
                            ImhotepProtobufShipping.readGroupMultiRemapMessage(is);
                          final GroupMultiRemapRule rule =
                            ImhotepDaemonMarshaller.marshal(message);
                          i++;
                          return rule;
                      } catch (final IOException e) {
                          throw Throwables.propagate(e);
                      }
                  }
            };
            final int numGroups =
                service.handleMultisplitRegroup(request.getSessionId(),
                                                numRules,
                                                it,
                                                request.getErrorOnCollisions());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private ImhotepResponse approximateTopTerms(
                final ImhotepRequest          request,
                final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final List<TermCount> topTerms =
                service.handleApproximateTopTerms(request.getSessionId(),
                                                  request.getField(),
                                                  request.getIsIntField(),
                                                  request.getK());
            builder.addAllTopTerms(ImhotepDaemonMarshaller.marshalTermCountList(topTerms));
            return builder.build();
        }

        private ImhotepResponse getPerformanceStats(
                final ImhotepRequest request,
                final ImhotepResponse.Builder builder) {
            final PerformanceStats stats =
                    service.handleGetPerformanceStats(
                            request.getSessionId(),
                            request.getResetPerformanceStats());
            builder.setPerformanceStats(ImhotepDaemonMarshaller.marshal(stats));
            return builder.build();
        }

        private void shutdown(
                final ImhotepRequest request,
                final InputStream    is,
                final OutputStream   os)
            throws IOException {
            if (request.hasSessionId() &&
                "magicshutdownid".equals(request.getSessionId())) {
                log.info("shutdown signal received, shutting down the JVM");
                close(socket, is, os);
                ImhotepDaemon.this.shutdown(true);
            }
        }

        private void internalRun() {
            ImhotepRequest request = null;
            try {
                final long beginTm = System.currentTimeMillis();

                final String remoteAddr = socket.getInetAddress().getHostAddress();

                final InputStream  is = Streams.newBufferedInputStream(socket.getInputStream());
                final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());

                final int  ndcDepth  = NDC.getDepth();
                final long requestId = requestIdCounter.incrementAndGet();

                ImhotepResponse response = null;
                GroupStatsIterator groupStats = null;

                NDC.push("#" + requestId);

                try {
                    log.debug("getting request");
                    // TODO TODO TODO validate request
                    request = ImhotepProtobufShipping.readRequest(is);

                    if (request.hasSessionId()) {
                        NDC.push(request.getSessionId());
                    }

                    log.debug("received request of type " + request.getRequestType() +
                             ", building response");
                    final ImhotepResponse.Builder builder = ImhotepResponse.newBuilder();
                    switch (request.getRequestType()) {
                        case OPEN_SESSION:
                            response = openSession(request, builder);
                            break;
                        case CLOSE_SESSION:
                            response = closeSession(request, builder);
                            break;
                        case REGROUP:
                            response = regroup(request, builder);
                            break;
                        case EXPLODED_REGROUP:
                            response = explodedRegroup(request, builder, is);
                            break;
                        case QUERY_REGROUP:
                            response = queryRegroup(request, builder);
                            break;
                        case INT_OR_REGROUP:
                            response = intOrRegroup(request, builder);
                            break;
                        case STRING_OR_REGROUP:
                            response = stringOrRegroup(request, builder);
                            break;
                        case RANDOM_REGROUP:
                            response = randomRegroup(request, builder);
                            break;
                        case RANDOM_MULTI_REGROUP:
                            response = randomMultiRegroup(request, builder);
                            break;
                        case RANDOM_METRIC_REGROUP:
                            response = randomMetricRegroup(request, builder);
                            break;
                        case RANDOM_METRIC_MULTI_REGROUP:
                            response = randomMetricMultiRegroup(request, builder);
                            break;
                        case REGEX_REGROUP:
                            response = regexRegroup(request, builder);
                            break;
                        case GET_TOTAL_DOC_FREQ:
                            response = getTotalDocFreq(request, builder);
                            break;
                        case GET_GROUP_STATS:
                            response = getGroupStats(request, builder);
                            break;
                        case STREAMING_GET_GROUP_STATS:
                            final Pair<ImhotepResponse, GroupStatsIterator> responseAndStat = getStreamingGroupStats(request, builder);
                            response = responseAndStat.getFirst();
                            groupStats = Preconditions.checkNotNull(responseAndStat.getSecond());
                            break;
                        case GET_FTGS_ITERATOR:
                            getFTGSIterator(request, builder, os);
                            break;
                        case GET_SUBSET_FTGS_ITERATOR:
                            getSubsetFTGSIterator(request, builder, os);
                            break;
                        case GET_FTGS_SPLIT:
                            getFTGSSplit(request, builder, os);
                            break;
                        case GET_SUBSET_FTGS_SPLIT:
                            getSubsetFTGSSplit(request, builder, os);
                            break;
                        case MERGE_FTGS_SPLIT:
                            mergeFTGSSplit(request, builder, os);
                            break;
                        case MERGE_SUBSET_FTGS_SPLIT:
                            mergeSubsetFTGSSplit(request, builder, os);
                            break;
                        case PUSH_STAT:
                            response = pushStat(request, builder);
                            break;
                        case POP_STAT:
                            response = popStat(request, builder);
                            break;
                        case GET_NUM_GROUPS:
                            response = getNumGroups(request, builder);
                            break;
                        case GET_SHARD_INFO_LIST:
                            response = getShardInfoList(request, builder);
                            break;
                        case GET_SHARD_LIST_FOR_TIME:
                            response = getShardlistForTime(request, builder);
                            break;
                        case GET_DATASET_METADATA:
                            response = getDatasetMetadata(request, builder);
                            break;
                        case GET_STATUS_DUMP:
                            response = getStatusDump(request, builder);
                            break;
                        case METRIC_REGROUP:
                            response = metricRegroup(request, builder);
                            break;
                        case METRIC_REGROUP_2D:
                            response = metricRegroup2D(request, builder);
                            break;
                        case METRIC_FILTER:
                            response = metricFilter(request, builder);
                            break;
                        case CREATE_DYNAMIC_METRIC:
                            response = createDynamicMetric(request, builder);
                            break;
                        case UPDATE_DYNAMIC_METRIC:
                            response = updateDynamicMetric(request, builder);
                            break;
                        case CONDITIONAL_UPDATE_DYNAMIC_METRIC:
                            response = conditionalUpdateDynamicMetric(request, builder);
                            break;
                        case GROUP_CONDITIONAL_UPDATE_DYNAMIC_METRIC:
                            response = groupConditionalUpdateDynamicMetric(request, builder);
                            break;
                        case GROUP_QUERY_UPDATE_DYNAMIC_METRIC:
                            response = groupQueryUpdateDynamicMetric(request, builder);
                            break;
                        case OPTIMIZE_SESSION:
                            response = optimizeSession(request, builder);
                            break;
                        case RESET_GROUPS:
                            response = resetGroups(request, builder);
                            break;
                        case MULTISPLIT_REGROUP:
                            response = multisplitRegroup(request, builder);
                            break;
                        case EXPLODED_MULTISPLIT_REGROUP:
                            response = explodedMultisplitRegroup(request, builder, is);
                            break;
                        case APPROXIMATE_TOP_TERMS:
                            response = approximateTopTerms(request, builder);
                            break;
                        case GET_PERFORMANCE_STATS:
                            response = getPerformanceStats(request, builder);
                            break;
                        case GET_DISTINCT:
                            final Pair<ImhotepResponse, GroupStatsIterator> responseAndDistinct = getDistinct(request, builder);
                            response = responseAndDistinct.getFirst();
                            groupStats = Preconditions.checkNotNull(responseAndDistinct.getSecond());
                            break;
                        case MERGE_DISTINCT_SPLIT:
                            final Pair<ImhotepResponse, GroupStatsIterator> responseAndDistinctSplit = mergeDistinctSplit(request, builder);
                            response = responseAndDistinctSplit.getFirst();
                            groupStats = Preconditions.checkNotNull(responseAndDistinctSplit.getSecond());
                            break;
                        case SHUTDOWN:
                            shutdown(request, is, os);
                            break;
                        default:
                            throw new IllegalArgumentException("unsupported request type: " +
                                                               request.getRequestType());
                    }
                    if (response != null) {
                        sendResponse(response, os);
                        if( groupStats != null ) {
                            sendGroupStat(groupStats, os);
                        }
                    }
                } catch (final ImhotepOutOfMemoryException e) {
                    expireSession(request, e);
                    final ImhotepResponse.ResponseCode oom =
                        ImhotepResponse.ResponseCode.OUT_OF_MEMORY;
                    sendResponse(ImhotepResponse.newBuilder().setResponseCode(oom).build(), os);
                    log.warn("ImhotepOutOfMemoryException while servicing request", e);
                } catch (final IOException e) {
                    try {
                        sendResponse(newErrorResponse(e), os);
                    } catch (final Exception e2) {
                        log.error("Exception during sending back the error", e2);
                    }
                    throw e;
                } catch (final RuntimeException e) {
                    expireSession(request, e);
                    try {
                        sendResponse(newErrorResponse(e), os);
                    } catch (final Exception e2) {
                        log.error("Exception during sending back the error", e2);
                    }
                    throw e;
                } finally {
                    // try to make the volume of logs manageable by skipping GET_FTGS_SPLIT requests
                    if(request != null && request.getRequestType() != ImhotepRequest.RequestType.GET_FTGS_SPLIT) {
                        final long endTm = System.currentTimeMillis();
                        final long elapsedTm = endTm - beginTm;
                        final DaemonEvents.HandleRequestEvent instEvent =
                                request.getRequestType().equals(ImhotepRequest.RequestType.OPEN_SESSION) ?
                                        new DaemonEvents.OpenSessionEvent(request, response,
                                                remoteAddr, localAddr,
                                                beginTm, elapsedTm) :
                                        new DaemonEvents.HandleRequestEvent(request, response,
                                                remoteAddr, localAddr,
                                                beginTm, elapsedTm);
                        instrumentation.fire(instEvent);
                    }
                    NDC.setMaxDepth(ndcDepth);
                    close(socket, is, os);
                }
            } catch (final IOException e) {
                expireSession(request,e );
                if (e instanceof SocketException) {
                    log.warn("IOException while servicing request", e);
                } else {
                    log.error("IOException while servicing request", e);
                }
                throw new RuntimeException(e);
            }
        }

        private void checkSessionValidity(final ImhotepRequest protoRequest) {
            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                throw new IllegalArgumentException("invalid session: " +
                                                   protoRequest.getSessionId());
            }
        }

        private ImhotepResponse newErrorResponse(final Exception e) {
            return ImhotepResponse.newBuilder()
                    .setResponseCode(ImhotepResponse.ResponseCode.OTHER_ERROR)
                    .setExceptionType(e.getClass().getName())
                    .setExceptionMessage(e.getMessage() != null ? e.getMessage() : "")
                    .setExceptionStackTrace(Throwables.getStackTraceAsString(e))
                    .build();
        }

        private void expireSession(final ImhotepRequest protoRequest, final Exception reason) {
            if (protoRequest != null && protoRequest.hasSessionId()) {
                final String sessionId = protoRequest.getSessionId();
                log.info("exception caught, closing session "+sessionId);
                try {
                    service.handleCloseSession(sessionId);
                } catch (final RuntimeException e) {
                    log.warn(e);
                }
            }
        }
    }

    public void shutdown(final boolean sysExit) throws IOException {
        if (zkWrapper != null) {
            zkWrapper.close();
        }
        if (!ss.isClosed()) {
            try {
                ss.close();
            } catch (final IOException e) {
                log.error("error closing server socket", e);
            }
        }
        executor.shutdownNow();
        service.close();
        if (sysExit) {
            System.exit(0);
        }
    }

    private static String[] getStringFields(final ImhotepRequest protoRequest) {
        return protoRequest.getStringFieldsList().toArray(new String[protoRequest.getStringFieldsCount()]);
    }

    private static String[] getIntFields(final ImhotepRequest protoRequest) {
        return protoRequest.getIntFieldsList().toArray(new String[protoRequest.getIntFieldsCount()]);
    }

    private static Map<String, long[]> getIntFieldsToTerms(final ImhotepRequest protoRequest) {
        final LinkedHashMap<String, long[]> ret = Maps.newLinkedHashMap();
        final List<IntFieldAndTerms> intFieldsToTermsList = protoRequest.getIntFieldsToTermsList();
        for (final IntFieldAndTerms intFieldAndTerms : intFieldsToTermsList) {
            final long[] array = new long[intFieldAndTerms.getTermsCount()];
            for (int i = 0; i < array.length; i++) {
                array[i] = intFieldAndTerms.getTerms(i);
            }
            ret.put(intFieldAndTerms.getField(), array);
        }
        return ret;
    }

    private static Map<String, String[]> getStringFieldsToTerms(final ImhotepRequest protoRequest) {
        final LinkedHashMap<String, String[]> ret = Maps.newLinkedHashMap();
        final List<StringFieldAndTerms> stringFieldsToTermsList = protoRequest.getStringFieldsToTermsList();
        for (final StringFieldAndTerms stringFieldAndTerms : stringFieldsToTermsList) {
            final String[] array = new String[stringFieldAndTerms.getTermsCount()];
            for (int i = 0; i < array.length; i++) {
                array[i] = stringFieldAndTerms.getTerms(i);
            }
            ret.put(stringFieldAndTerms.getField(), array);
        }
        return ret;
    }

    private static void close(final Socket socket, final InputStream is, final OutputStream os) {
        try {
            is.close();
        } catch (final IOException e) {
            log.warn("error closing SocketInputStream", e);
        }
        try {
            os.close();
        } catch (final IOException e) {
            log.warn("error closing SocketOutputStream", e);
        }
        try {
            socket.close();
        } catch (final IOException e) {
            log.warn("error closing Socket", e);
        }
    }

    private static void close(final Socket socket, final OutputStream os) {
        try {
            os.close();
        } catch (final IOException e) {
            log.warn("error closing SocketOutputStream", e);
        }
        try {
            socket.close();
        } catch (final IOException e) {
            log.warn("error closing Socket", e);
        }
    }

    public static void main(final String[] args) throws IOException, URISyntaxException {
        if (args.length < 1) {
            System.err.println("ARGS: shardDir tempDir [--port port] [--memory memory] "
                    + "[--zknodes zknodes] [--zkport zkport] [--lazyLoadProps <properties file>]");
            System.exit(1);
        }

        final String shardsDirectory = args[0];
        final String tempDirectory = args[1];
        int port = 9000;
        long memoryCapacityInMB = 1024;
        boolean shutdown = false;
        String zkNodes = null;
        String zkPath = null;
        for (int i = 2; i < args.length; ++i) {
            switch (args[i]) {
                case "--port":
                    port = Integer.parseInt(args[++i]);
                    break;
                case "--memory":
                    memoryCapacityInMB = Long.parseLong(args[++i]);
                    break;
                case "--shutdown":
                    shutdown = true;
                    break;
                case "--zknodes":
                    zkNodes = args[++i];
                    break;
                case "--zkpath":
                    zkPath = args[++i];
                    break;
                default:
                    throw new RuntimeException("unrecognized arg: " + args[i]);
            }
        }

        if (shutdown) {
            shutdownLocalhostDaemon(port);
        } else {
            main(shardsDirectory,
                 tempDirectory,
                 port,
                 memoryCapacityInMB,
                 zkNodes,
                 zkPath);
        }
    }

    public static void shutdownLocalhostDaemon(final int port) throws IOException {
        final Socket socket = new Socket("localhost", port);
        final OutputStream os = socket.getOutputStream();
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.SHUTDOWN)
                .setSessionId("magicshutdownid")
                .build();
        ImhotepProtobufShipping.sendProtobuf(request, os);
        close(socket, os);
    }

    public static void main(final String shardsDirectory,
                            final String tempDirectory,
                            final int port,
                            final long memoryCapacityInMB,
                            final String zkNodes,
                            final String zkPath) throws IOException, URISyntaxException {
        ImhotepDaemon daemon = null;
        try {
            daemon = newImhotepDaemon(shardsDirectory,
                                      tempDirectory,
                                      port,
                                      memoryCapacityInMB,
                                      zkNodes,
                                      zkPath,
                                      null,
                                      null);
            daemon.run();
        } finally {
            if (daemon != null) {
                daemon.shutdown(false);
            }
        }
    }

    private ServiceCoreObserver getServiceCoreObserver() {
        if (serviceCoreObserver == null) {
            serviceCoreObserver = new ServiceCoreObserver();
        }
        return serviceCoreObserver;
    }

    static ImhotepDaemon newImhotepDaemon(final String shardsDirectory,
                                          final String shardTempDir,
                                          final int port,
                                          final long memoryCapacityInMB,
                                          final String zkNodes,
                                          final String zkPath,
                                          final @Nullable Integer sessionForwardingPort,
                                          @Nullable LocalImhotepServiceConfig localImhotepServiceConfig) throws IOException, URISyntaxException {
        final AbstractImhotepServiceCore localService;
        final ShardUpdateListener shardUpdateListener = new ShardUpdateListener();

        // initialize the imhotepfs if necessary
        RemoteCachingFileSystemProvider.newFileSystem();

        final Path shardsDir = NioPathUtil.get(shardsDirectory);
        final Path tmpDir = NioPathUtil.get(shardTempDir);

        final String myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        final ServerSocket ss = new ServerSocket(port);
        final Host myHost = new Host(myHostname, ss.getLocalPort());
        final Supplier<ShardMaster> shardMasterSupplier = getShardMasterSupplier(zkNodes, myHost);

        if(localImhotepServiceConfig == null) {
            localImhotepServiceConfig = new LocalImhotepServiceConfig();
        }

        localService = new LocalImhotepServiceCore(shardsDir,
                                                   tmpDir,
                                                   memoryCapacityInMB * 1024 * 1024,
                                                   new GenericFlamdexReaderSource(),
                                                   new ShardDirIteratorFactory(shardMasterSupplier, myHost),
                                                   localImhotepServiceConfig,
                                                   shardUpdateListener);
        final ImhotepDaemon result =
            new ImhotepDaemon(ss, localService, zkNodes, zkPath, myHostname, port, shardUpdateListener, sessionForwardingPort);
        localService.addObserver(result.getServiceCoreObserver());
        return result;
    }

    private static Supplier<ShardMaster> getShardMasterSupplier(String zkNodes, Host myHost) {
        final String shardMasterHost = System.getProperty("imhotep.shardmaster.host");
        if(shardMasterHost != null) {
            // we have an exact host:port combination provided so use that
            return () -> new RequestResponseClient(
                    new Host(shardMasterHost.split(":")[0],
                    Integer.valueOf(shardMasterHost.split(":")[1]))
            );
        } else {
            // Use ZooKeeper to locate ShardMaster
            return new RequestResponseClientFactory(zkNodes, System.getProperty("imhotep.shardmaster.zookeeper.path"), myHost);
        }
    }

    public ImhotepServiceCore getService() {
        return service;
    }

    public ServiceZooKeeperWrapper getZkWrapper() {
        return zkWrapper;
    }

    public int getPort() {
        return ss.getLocalPort();
    }
}
