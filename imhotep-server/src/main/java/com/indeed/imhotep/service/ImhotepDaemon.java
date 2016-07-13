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
package com.indeed.imhotep.service;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.GroupMultiRemapRule;
import com.indeed.imhotep.GroupRemapRule;
import com.indeed.imhotep.ImhotepStatusDump;
import com.indeed.imhotep.Instrumentation;
import com.indeed.imhotep.Instrumentation.Keys;
import com.indeed.imhotep.RegroupCondition;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.protobuf.IntFieldAndTerms;
import com.indeed.imhotep.protobuf.QueryRemapMessage;
import com.indeed.imhotep.protobuf.RegroupConditionMessage;
import com.indeed.imhotep.protobuf.StringFieldAndTerms;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

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

        private final AtomicReference<ImhotepResponse> shardListResponse =
            new AtomicReference<ImhotepResponse>();

        private final AtomicReference<ImhotepResponse> datasetListResponse =
            new AtomicReference<ImhotepResponse>();

        public void onShardUpdate(final List<ShardInfo> shardList,
                                  final Source unusedSource) {
            final ImhotepResponse.Builder builder = ImhotepResponse.newBuilder();
            for (final ShardInfo shard : shardList) {
                builder.addShardInfo(shard.toProto());
            }
            final ImhotepResponse response = builder.build();
            shardListResponse.set(response);
        }

        public void onDatasetUpdate(final List<DatasetInfo> datasetList,
                                    final Source unusedSource) {
            final ImhotepResponse.Builder builder = ImhotepResponse.newBuilder();
            for (final DatasetInfo dataset : datasetList) {
                builder.addDatasetInfo(dataset.toProto());
            }
            final ImhotepResponse response = builder.build();
            datasetListResponse.set(response);
        }

        public ImhotepResponse   getShardListResponse() { return shardListResponse.get();   }
        public ImhotepResponse getDatasetListResponse() { return datasetListResponse.get(); }
    }

    /* Relays events to our observers. */
    private final class ServiceCoreObserver implements Instrumentation.Observer {
        public void onEvent(Instrumentation.Event event) {
            instrumentation.fire(event);
        }
    }

    public ImhotepDaemon(ServerSocket ss, AbstractImhotepServiceCore service,
                         String zkNodes, String zkPath, String hostname, int port,
                         ShardUpdateListener shardUpdateListener) {
        this.ss = ss;
        this.service = service;
        this.shardUpdateListener = shardUpdateListener;
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ImhotepDaemonRemoteServiceThread"+i++);
            }
        });

        zkWrapper = zkNodes != null ?
            new ServiceZooKeeperWrapper(zkNodes, hostname, port, zkPath) : null;
    }

    /** Intended for tests that create their own ImhotepDaemons. */
    public ImhotepDaemon(ServerSocket ss, AbstractImhotepServiceCore service,
                         String zkNodes, String zkPath, String hostname, int port) {
        this(ss, service, zkNodes, zkPath, hostname, port, new ShardUpdateListener());
    }

    public void addObserver(Instrumentation.Observer observer) {
        instrumentation.addObserver(observer);
    }

    public void removeObserver(Instrumentation.Observer observer) {
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
                } catch (IOException e) {
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

    public void waitForStartup(long timeout) throws TimeoutException {
        long startTime = System.currentTimeMillis();
        while (!isStarted() && (System.currentTimeMillis() - startTime) < timeout) {}
        if (!isStarted()) {
            throw new TimeoutException("ImhotepDaemon failed to start within " + timeout + " ms");
        }
    }

    static void sendResponse(ImhotepResponse response, OutputStream os) throws IOException {
        log.debug("sending response");
        ImhotepProtobufShipping.sendProtobuf(response, os);
        log.debug("response sent");
    }

    private class DaemonWorker implements Runnable {
        private final Socket socket;
        private final String localAddr;

        private DaemonWorker(Socket socket) {
            this.socket = socket;

            String tmpAddr;
            try {
                tmpAddr = InetAddress.getLocalHost().toString();
            }
            catch (Exception ex) {
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
            } catch (RuntimeException e) {
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

        private final ImhotepResponse openSession(final ImhotepRequest          request,
                                                  final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
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
                                          request.getUseNativeFtgs(),
                                          request.getSessionTimeout());
            NDC.push(sessionId);
            builder.setSessionId(sessionId);
            return builder.build();
        }

        private final ImhotepResponse closeSession(final ImhotepRequest          request,
                                                   final ImhotepResponse.Builder builder) {
            service.handleCloseSession(request.getSessionId());
            return builder.build();
        }

        private final ImhotepResponse regroup(final ImhotepRequest          request,
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

        private final ImhotepResponse explodedRegroup(final ImhotepRequest          request,
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
                                              } catch (IOException e) {
                                                  throw Throwables.propagate(e);
                                              }
                                          }
                                      });
            return builder.setNumGroups(numGroups).build();
        }

        private final ImhotepResponse queryRegroup(final ImhotepRequest          request,
                                                   final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final QueryRemapMessage remapMessage = request.getQueryRemapRule();
            final int numGroups =
                service.handleQueryRegroup(request.getSessionId(),
                                           ImhotepDaemonMarshaller.marshal(remapMessage));
            return builder.setNumGroups(numGroups).build();
        }

        private final ImhotepResponse intOrRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse stringOrRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse randomRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse randomMultiRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse regexRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse getTotalDocFreq(final ImhotepRequest          request,
                                                      final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final long totalDocFreq =
                service.handleGetTotalDocFreq(request.getSessionId(),
                                              getIntFields(request),
                                              getStringFields(request));
            builder.setTotalDocFreq(totalDocFreq);
            return builder.build();
        }

        private final ImhotepResponse getGroupStats(final ImhotepRequest          request,
                                                    final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            long[] groupStats =
                service.handleGetGroupStats(request.getSessionId(),
                                            request.getStat());
            for (final long groupStat : groupStats) {
                builder.addGroupStat(groupStat);
            }
            return builder.build();
        }

        private final void getFTGSIterator(final ImhotepRequest          request,
                                           final ImhotepResponse.Builder builder,
                                           final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetFTGSIterator(request.getSessionId(),
                                          getIntFields(request),
                                          getStringFields(request),
                                          request.getTermLimit(),
                                          request.getSortStat(),
                                          os);
        }

        private final void getSubsetFTGSIterator(final ImhotepRequest          request,
                                                 final ImhotepResponse.Builder builder,
                                                 final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetSubsetFTGSIterator(request.getSessionId(),
                                                getIntFieldsToTerms(request),
                                                getStringFieldsToTerms(request),
                                                os);
        }

        private final void getFTGSSplit(final ImhotepRequest          request,
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

        private final void getFTGSSplitNative(final ImhotepRequest          request,
                                              final ImhotepResponse.Builder builder,
                                              final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetFTGSIteratorSplitNative(request.getSessionId(),
                                                     getIntFields(request),
                                                     getStringFields(request),
                                                     os,
                                                     request.getSplitIndex(),
                                                     request.getNumSplits(),
                                                     request.getTermLimit(),
                                                     socket);
        }

        private final void getSubsetFTGSSplit(final ImhotepRequest          request,
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

        private final void mergeFTGSSplit(final ImhotepRequest          request,
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
            service.handleMergeFTGSIteratorSplit(request.getSessionId(),
                                                 getIntFields(request),
                                                 getStringFields(request),
                                                 os, nodes, request.getSplitIndex(), request.getTermLimit(), request.getSortStat());
        }

        private final void mergeSubsetFTGSSplit(final ImhotepRequest          request,
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

        private final void getDocIterator(final ImhotepRequest          request,
                                          final ImhotepResponse.Builder builder,
                                          final OutputStream            os)
            throws IOException, ImhotepOutOfMemoryException {
            checkSessionValidity(request);
            service.handleGetDocIterator(request.getSessionId(),
                                         getIntFields(request),
                                         getStringFields(request), os);
        }

        private final ImhotepResponse pushStat(final ImhotepRequest          request,
                                               final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numStats = service.handlePushStat(request.getSessionId(),
                                                        request.getMetric());
            builder.setNumStats(numStats);
            return builder.build();
        }

        private final ImhotepResponse popStat(final ImhotepRequest          request,
                                              final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numStats = service.handlePopStat(request.getSessionId());
            builder.setNumStats(numStats);
            return builder.build();
        }

        private final ImhotepResponse getNumGroups(final ImhotepRequest          request,
                                                   final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final int numGroups = service.handleGetNumGroups(request.getSessionId());
            builder.setNumGroups(numGroups);
            return builder.build();
        }

        private final ImhotepResponse getShardList(final ImhotepRequest          request,
                                                   final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            ImhotepResponse response = shardUpdateListener.getShardListResponse();
            if (response == null) {
                final List<ShardInfo> shards = service.handleGetShardList();
                for (final ShardInfo shard : shards) {
                    builder.addShardInfo(shard.toProto());
                }
                response = builder.build();
            }
            return response;
        }

        private final ImhotepResponse getShardInfoList(final ImhotepRequest          request,
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

        private final ImhotepResponse getStatusDump(final ImhotepRequest          request,
                                                    final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            final ImhotepStatusDump statusDump = service.handleGetStatusDump();
            builder.setStatusDump(statusDump.toProto());
            return builder.build();
        }

        private final ImhotepResponse metricRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse metricRegroup2D(final ImhotepRequest          request,
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

        private final ImhotepResponse metricFilter(final ImhotepRequest          request,
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

        private final ImhotepResponse createDynamicMetric(final ImhotepRequest          request,
                                                          final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleCreateDynamicMetric(request.getSessionId(),
                                              request.getDynamicMetricName());
            return builder.build();
        }

        private final ImhotepResponse updateDynamicMetric(final ImhotepRequest          request,
                                                          final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleUpdateDynamicMetric(request.getSessionId(),
                                              request.getDynamicMetricName(),
                                              Ints.toArray(request.getDynamicMetricDeltasList()));
            return builder.build();
        }

        private final ImhotepResponse
        conditionalUpdateDynamicMetric(final ImhotepRequest          request,
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

        private final ImhotepResponse
        groupConditionalUpdateDynamicMetric(final ImhotepRequest          request,
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

        private final ImhotepResponse optimizeSession(final ImhotepRequest          request,
                                                      final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleRebuildAndFilterIndexes(request.getSessionId(),
                                                  getIntFields(request),
                                                  getStringFields(request));
            return builder.build();
        }

        private final ImhotepResponse resetGroups(final ImhotepRequest          request,
                                                  final ImhotepResponse.Builder builder)
            throws ImhotepOutOfMemoryException {
            service.handleResetGroups(request.getSessionId());
            return builder.build();
        }

        private final ImhotepResponse multisplitRegroup(final ImhotepRequest          request,
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

        private final ImhotepResponse
            explodedMultisplitRegroup(final ImhotepRequest          request,
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
                      } catch (IOException e) {
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

        private final ImhotepResponse approximateTopTerms(final ImhotepRequest          request,
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

        private final void shutdown(final ImhotepRequest request,
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
                        case REGEX_REGROUP:
                            response = regexRegroup(request, builder);
                            break;
                        case GET_TOTAL_DOC_FREQ:
                            response = getTotalDocFreq(request, builder);
                            break;
                        case GET_GROUP_STATS:
                            response = getGroupStats(request, builder);
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
                        case GET_FTGS_SPLIT_NATIVE:
                            getFTGSSplitNative(request, builder, os);
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
                        case GET_DOC_ITERATOR:
                            getDocIterator(request, builder, os);
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
                        case GET_SHARD_LIST:
                            response = getShardList(request, builder);
                            break;
                        case GET_SHARD_INFO_LIST:
                            response = getShardInfoList(request, builder);
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
                        case SHUTDOWN:
                            shutdown(request, is, os);
                            break;
                        default:
                            throw new IllegalArgumentException("unsupported request type: " +
                                                               request.getRequestType());
                    }
                    if (response != null) {
                        sendResponse(response, os);
                    }
                } catch (ImhotepOutOfMemoryException e) {
                    expireSession(request, e);
                    final ImhotepResponse.ResponseCode oom =
                        ImhotepResponse.ResponseCode.OUT_OF_MEMORY;
                    sendResponse(ImhotepResponse.newBuilder().setResponseCode(oom).build(), os);
                    log.warn("ImhotepOutOfMemoryException while servicing request", e);
                } catch (IOException e) {
                    sendResponse(newErrorResponse(e), os);
                    throw e;
                } catch (RuntimeException e) {
                    expireSession(request, e);
                    sendResponse(newErrorResponse(e), os);
                    throw e;
                } finally {
                    final long endTm = System.currentTimeMillis();
                    final long elapsedTm = endTm - beginTm;
                    DaemonEvents.HandleRequestEvent instEvent =
                        request.getRequestType().equals(ImhotepRequest.RequestType.OPEN_SESSION) ?
                        new DaemonEvents.OpenSessionEvent(request, response,
                                                          remoteAddr, localAddr,
                                                          beginTm, elapsedTm) :
                        new DaemonEvents.HandleRequestEvent(request, response,
                                                            remoteAddr, localAddr,
                                                            beginTm, elapsedTm);
                    instrumentation.fire(instEvent);
                    NDC.setMaxDepth(ndcDepth);
                    close(socket, is, os);
                }
            } catch (IOException e) {
                expireSession(request,e );
                if (e instanceof SocketException) {
                    log.warn("IOException while servicing request", e);
                } else {
                    log.error("IOException while servicing request", e);
                }
                throw new RuntimeException(e);
            }
        }

        private void checkSessionValidity(ImhotepRequest protoRequest) {
            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                throw new IllegalArgumentException("invalid session: " +
                                                   protoRequest.getSessionId());
            }
        }

        private ImhotepResponse newErrorResponse(Exception e) {
            return ImhotepResponse.newBuilder()
                    .setResponseCode(ImhotepResponse.ResponseCode.OTHER_ERROR)
                    .setExceptionType(e.getClass().getName())
                    .setExceptionMessage(e.getMessage() != null ? e.getMessage() : "")
                    .setExceptionStackTrace(Throwables.getStackTraceAsString(e))
                    .build();
        }

        private void expireSession(ImhotepRequest protoRequest, Exception reason) {
            if (protoRequest != null && protoRequest.hasSessionId()) {
                final String sessionId = protoRequest.getSessionId();
                log.info("exception caught, closing session "+sessionId);
                try {
                    service.handleCloseSession(sessionId);
                } catch (RuntimeException e) {
                    log.warn(e);
                }
            }
        }
    }

    public void shutdown(boolean sysExit) throws IOException {
        if (zkWrapper != null) {
            zkWrapper.close();
        }
        if (!ss.isClosed()) {
            try {
                ss.close();
            } catch (IOException e) {
                log.error("error closing server socket", e);
            }
        }
        executor.shutdownNow();
        service.close();
        if (sysExit) {
            System.exit(0);
        }
    }

    private static String[] getStringFields(ImhotepRequest protoRequest) {
        return protoRequest.getStringFieldsList().toArray(new String[protoRequest.getStringFieldsCount()]);
    }

    private static String[] getIntFields(ImhotepRequest protoRequest) {
        return protoRequest.getIntFieldsList().toArray(new String[protoRequest.getIntFieldsCount()]);
    }

    private static Map<String, long[]> getIntFieldsToTerms(ImhotepRequest protoRequest) {
        final LinkedHashMap<String, long[]> ret = Maps.newLinkedHashMap();
        final List<IntFieldAndTerms> intFieldsToTermsList = protoRequest.getIntFieldsToTermsList();
        for (IntFieldAndTerms intFieldAndTerms : intFieldsToTermsList) {
            final long[] array = new long[intFieldAndTerms.getTermsCount()];
            for (int i = 0; i < array.length; i++) {
                array[i] = intFieldAndTerms.getTerms(i);
            }
            ret.put(intFieldAndTerms.getField(), array);
        }
        return ret;
    }

    private static Map<String, String[]> getStringFieldsToTerms(ImhotepRequest protoRequest) {
        final LinkedHashMap<String, String[]> ret = Maps.newLinkedHashMap();
        final List<StringFieldAndTerms> stringFieldsToTermsList = protoRequest.getStringFieldsToTermsList();
        for (StringFieldAndTerms stringFieldAndTerms : stringFieldsToTermsList) {
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
        } catch (IOException e) {
            log.warn("error closing SocketInputStream", e);
        }
        try {
            os.close();
        } catch (IOException e) {
            log.warn("error closing SocketOutputStream", e);
        }
        try {
            socket.close();
        } catch (IOException e) {
            log.warn("error closing Socket", e);
        }
    }

    private static void close(final Socket socket, final OutputStream os) {
        try {
            os.close();
        } catch (IOException e) {
            log.warn("error closing SocketOutputStream", e);
        }
        try {
            socket.close();
        } catch (IOException e) {
            log.warn("error closing Socket", e);
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("ARGS: shardDir tempDir [--port port] [--memory memory] "
                    + "[--zknodes zknodes] [--zkport zkport] [--lazyLoadProps <properties file>]");
            System.exit(1);
        }

        final String shardsDirectory = args[0];
        final String tempDirectory = args[1];
        int port = 9000;
        long memoryCapacityInMB = 1024;
        boolean useCache = false;
        boolean shutdown = false;
        String zkNodes = null;
        String zkPath = null;
        for (int i = 2; i < args.length; ++i) {
            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[++i]);
            } else if (args[i].equals("--memory")) {
                memoryCapacityInMB = Long.parseLong(args[++i]);
            } else if (args[i].equals("--shutdown")) {
                shutdown = true;
            } else if (args[i].equals("--zknodes")) {
                zkNodes = args[++i];
            } else if (args[i].equals("--zkpath")) {
                zkPath = args[++i];
            } else if (args[i].equals("--cache")) {
                useCache = true;
            } else {
                throw new RuntimeException("unrecognized arg: "+args[i]);
            }
        }

        if (shutdown) {
            shutdownLocalhostDaemon(port);
        } else {
            main(shardsDirectory,
                 tempDirectory,
                 port,
                 memoryCapacityInMB,
                 useCache,
                 zkNodes,
                 zkPath);
        }
    }

    public static void shutdownLocalhostDaemon(int port) throws IOException {
        final Socket socket = new Socket("localhost", port);
        final OutputStream os = socket.getOutputStream();
        final ImhotepRequest request = ImhotepRequest.newBuilder()
                .setRequestType(ImhotepRequest.RequestType.SHUTDOWN)
                .setSessionId("magicshutdownid")
                .build();
        ImhotepProtobufShipping.sendProtobuf(request, os);
        close(socket, os);
    }

    public static void main(String shardsDirectory,
                            String tempDirectory,
                            int port,
                            long memoryCapacityInMB,
                            boolean useCache,
                            String zkNodes,
                            String zkPath) throws IOException {
        ImhotepDaemon daemon = null;
        try {
            daemon = newImhotepDaemon(shardsDirectory,
                                      tempDirectory,
                                      port,
                                      memoryCapacityInMB,
                                      useCache,
                                      zkNodes,
                                      zkPath);
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

    static ImhotepDaemon newImhotepDaemon(String shardsDirectory,
                                          String shardTempDir,
                                          int port,
                                          long memoryCapacityInMB,
                                          boolean useCache,
                                          String zkNodes,
                                          String zkPath) throws IOException {
        final AbstractImhotepServiceCore localService;
        final ShardUpdateListener shardUpdateListener = new ShardUpdateListener();

        localService =
            new LocalImhotepServiceCore(shardsDirectory, shardTempDir,
                                        memoryCapacityInMB * 1024 * 1024, useCache,
                                        new GenericFlamdexReaderSource(),
                                        new LocalImhotepServiceConfig(),
                                        shardUpdateListener);
        final ServerSocket ss = new ServerSocket(port);
        final String myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        final ImhotepDaemon result =
            new ImhotepDaemon(ss, localService, zkNodes, zkPath,
                              myHostname, port, shardUpdateListener);
        localService.addObserver(result.getServiceCoreObserver());
        return result;
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
