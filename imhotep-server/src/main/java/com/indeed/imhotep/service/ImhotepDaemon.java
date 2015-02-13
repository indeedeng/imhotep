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
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.TermCount;
import com.indeed.imhotep.api.ImhotepOutOfMemoryException;
import com.indeed.imhotep.api.ImhotepServiceCore;
import com.indeed.imhotep.marshal.ImhotepDaemonMarshaller;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.GroupRemapMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;
import com.indeed.imhotep.io.ImhotepProtobufShipping;
import com.indeed.imhotep.io.Streams;
import com.indeed.imhotep.io.caching.CachedFile;

import com.indeed.imhotep.protobuf.IntFieldAndTerms;
import com.indeed.imhotep.protobuf.StringFieldAndTerms;
import org.apache.log4j.Logger;
import org.apache.log4j.NDC;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

public class ImhotepDaemon {
    private static final Logger log = Logger.getLogger(ImhotepDaemon.class);

    private final ServerSocket ss;

    private final ExecutorService executor;
    private final ImhotepServiceCore service;
    private final ServiceZooKeeperWrapper zkWrapper;

    private final AtomicLong requestIdCounter = new AtomicLong(0);

    private volatile boolean isStarted = false;

    public ImhotepDaemon(ServerSocket ss, ImhotepServiceCore service, String zkNodes, String zkPath, String hostname, int port) {
        this.ss = ss;
        this.service = service;
        executor = Executors.newCachedThreadPool(new ThreadFactory() {
            int i = 0;
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "ImhotepDaemonRemoteServiceThread"+i++);
            }
        });
        zkWrapper = zkNodes != null ? new ServiceZooKeeperWrapper(zkNodes, hostname, port, zkPath) : null;
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
                    log.info("received connection, running");
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
        log.info("sending response");
        ImhotepProtobufShipping.sendProtobuf(response, os);
        log.info("response sent");
    }

    private class DaemonWorker implements Runnable {
        private final Socket socket;

        private DaemonWorker(Socket socket) {
            this.socket = socket;
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

        private void internalRun() {
            ImhotepRequest protoRequest = null;
            try {
                final InputStream is = Streams.newBufferedInputStream(socket.getInputStream());
                final OutputStream os = Streams.newBufferedOutputStream(socket.getOutputStream());

                final int ndcDepth = NDC.getDepth();

                final long requestId = requestIdCounter.incrementAndGet();
                NDC.push("#" + requestId);

                try {
                    log.info("getting request");
                    // TODO TODO TODO validate request
                    protoRequest = ImhotepProtobufShipping.readRequest(is);

                    if (protoRequest.hasSessionId()) {
                        NDC.push(protoRequest.getSessionId());
                    }

                    log.info("received request of type "+protoRequest.getRequestType()+", building response");
                    final ImhotepResponse.Builder responseBuilder = ImhotepResponse.newBuilder();

                    InetAddress inetAddress;
                    String sessionId;
                    int numStats;
                    int numGroups;
                    List<ShardInfo> shards;
                    List<DatasetInfo> datasets;
                    long totalDocFreq;
                    long[] groupStats;
                    ImhotepStatusDump statusDump;
                    List<TermCount> topTerms;
                    switch (protoRequest.getRequestType()) {
                        case OPEN_SESSION:
                            inetAddress = socket.getInetAddress();
                            final AtomicLong tempFileSizeBytesLeft = protoRequest.getTempFileSizeLimit() > 0 ?
                                    new AtomicLong(protoRequest.getTempFileSizeLimit()) : null;
                            sessionId = service.handleOpenSession(
                                    protoRequest.getDataset(),
                                    protoRequest.getShardRequestList(),
                                    protoRequest.getUsername(),
                                    inetAddress.getHostAddress(),
                                    protoRequest.getClientVersion(),
                                    protoRequest.getMergeThreadLimit(),
                                    protoRequest.getOptimizeGroupZeroLookups(),
                                    protoRequest.getSessionId(),
                                    tempFileSizeBytesLeft
                            );
                            NDC.push(sessionId);
                            responseBuilder.setSessionId(sessionId);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case CLOSE_SESSION:
                            service.handleCloseSession(protoRequest.getSessionId());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case REGROUP:
                            numGroups = service.handleRegroup(protoRequest.getSessionId(), ImhotepDaemonMarshaller.marshalGroupRemapMessageList(protoRequest.getRemapRulesList()));
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case EXPLODED_REGROUP: {
                                final int numRules = protoRequest.getLength();
                                numGroups = service.handleRegroup(protoRequest.getSessionId(), numRules, new UnmodifiableIterator<GroupRemapRule>() {
                                    private int i = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return i < numRules;
                                    }

                                    @Override
                                    public GroupRemapRule next() {
                                        try {
                                            final GroupRemapMessage message = ImhotepProtobufShipping.readGroupRemapMessage(is);
                                            final GroupRemapRule rule = ImhotepDaemonMarshaller.marshal(message);
                                            i++;
                                            return rule;
                                        } catch (IOException e) {
                                            throw Throwables.propagate(e);
                                        }
                                    }
                                });
                                sendResponse(responseBuilder.setNumGroups(numGroups).build(), os);
                                break;
                            }
                        case QUERY_REGROUP:
                            numGroups = service.handleQueryRegroup(protoRequest.getSessionId(), ImhotepDaemonMarshaller.marshal(protoRequest.getQueryRemapRule()));
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case INT_OR_REGROUP:
                            service.handleIntOrRegroup(protoRequest.getSessionId(), protoRequest.getField(), Longs.toArray(protoRequest.getIntTermList()),
                                    protoRequest.getTargetGroup(), protoRequest.getNegativeGroup(), protoRequest.getPositiveGroup());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case STRING_OR_REGROUP:
                            service.handleStringOrRegroup(protoRequest.getSessionId(), protoRequest.getField(), protoRequest.getStringTermList().toArray(new String[protoRequest.getStringTermCount()]),
                                    protoRequest.getTargetGroup(), protoRequest.getNegativeGroup(), protoRequest.getPositiveGroup());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case RANDOM_REGROUP:
                            service.handleRandomRegroup(protoRequest.getSessionId(), protoRequest.getField(), protoRequest.getIsIntField(),
                                    protoRequest.getSalt(), protoRequest.getP(), protoRequest.getTargetGroup(), protoRequest.getNegativeGroup(),
                                    protoRequest.getPositiveGroup());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case RANDOM_MULTI_REGROUP:
                            service.handleRandomMultiRegroup(protoRequest.getSessionId(), protoRequest.getField(),
                                    protoRequest.getIsIntField(), protoRequest.getSalt(), protoRequest.getTargetGroup(),
                                    Doubles.toArray(protoRequest.getPercentagesList()),
                                    Ints.toArray(protoRequest.getResultGroupsList()));
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case REGEX_REGROUP:
                            service.handleRegexRegroup(protoRequest.getSessionId(), protoRequest.getField(), protoRequest.getRegex(),
                                    protoRequest.getTargetGroup(), protoRequest.getNegativeGroup(), protoRequest.getPositiveGroup());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_TOTAL_DOC_FREQ:
                            totalDocFreq = service.handleGetTotalDocFreq(
                                    protoRequest.getSessionId(),
                                    getIntFields(protoRequest),
                                    getStringFields(protoRequest)
                            );
                            responseBuilder.setTotalDocFreq(totalDocFreq);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_GROUP_STATS:
                            groupStats = service.handleGetGroupStats(protoRequest.getSessionId(), protoRequest.getStat());
                            for (final long groupStat : groupStats) {
                                responseBuilder.addGroupStat(groupStat);
                            }
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_FTGS_ITERATOR:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleGetFTGSIterator(protoRequest.getSessionId(), getIntFields(protoRequest), getStringFields(protoRequest), os);
                            break;
                        case GET_SUBSET_FTGS_ITERATOR:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleGetSubsetFTGSIterator(protoRequest.getSessionId(), getIntFieldsToTerms(protoRequest), getStringFieldsToTerms(protoRequest), os);
                            break;
                        case GET_FTGS_SPLIT:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleGetFTGSIteratorSplit(protoRequest.getSessionId(), getIntFields(protoRequest), getStringFields(protoRequest), os, protoRequest.getSplitIndex(), protoRequest.getNumSplits());
                            break;
                        case GET_SUBSET_FTGS_SPLIT:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleGetSubsetFTGSIteratorSplit(protoRequest.getSessionId(), getIntFieldsToTerms(protoRequest), getStringFieldsToTerms(protoRequest), os, protoRequest.getSplitIndex(), protoRequest.getNumSplits());
                            break;
                        case MERGE_FTGS_SPLIT:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleMergeFTGSIteratorSplit(protoRequest.getSessionId(), getIntFields(protoRequest), getStringFields(protoRequest), os,
                                    Lists.transform(protoRequest.getNodesList(), new Function<HostAndPort, InetSocketAddress>() {
                                        public InetSocketAddress apply(final HostAndPort input) {
                                            return new InetSocketAddress(input.getHost(), input.getPort());
                                        }
                                    }).toArray(new InetSocketAddress[protoRequest.getNodesCount()]), protoRequest.getSplitIndex());
                            break;
                        case MERGE_SUBSET_FTGS_SPLIT:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleMergeSubsetFTGSIteratorSplit(protoRequest.getSessionId(), getIntFieldsToTerms(protoRequest), getStringFieldsToTerms(protoRequest), os,
                                    Lists.transform(protoRequest.getNodesList(), new Function<HostAndPort, InetSocketAddress>() {
                                        public InetSocketAddress apply(final HostAndPort input) {
                                            return new InetSocketAddress(input.getHost(), input.getPort());
                                        }
                                    }).toArray(new InetSocketAddress[protoRequest.getNodesCount()]), protoRequest.getSplitIndex());
                            break;
                        case GET_DOC_ITERATOR:
                            if (!service.sessionIsValid(protoRequest.getSessionId())) {
                                throw new IllegalArgumentException("invalid session: " + protoRequest.getSessionId());
                            }
                            service.handleGetDocIterator(protoRequest.getSessionId(), getIntFields(protoRequest), getStringFields(protoRequest), os);
                            break;
                        case PUSH_STAT:
                            numStats = service.handlePushStat(protoRequest.getSessionId(), protoRequest.getMetric());
                            responseBuilder.setNumStats(numStats);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case POP_STAT:
                            numStats = service.handlePopStat(protoRequest.getSessionId());
                            responseBuilder.setNumStats(numStats);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_NUM_GROUPS:
                            numGroups = service.handleGetNumGroups(protoRequest.getSessionId());
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_SHARD_LIST:
                            shards = service.handleGetShardList();
                            for (final ShardInfo shard : shards) {
                                responseBuilder.addShardInfo(shard.toProto());
                            }
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_SHARD_INFO_LIST:
                            datasets = service.handleGetDatasetList();
                            for (final DatasetInfo dataset : datasets) {
                                responseBuilder.addDatasetInfo(dataset.toProto());
                            }
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GET_STATUS_DUMP:
                            statusDump = service.handleGetStatusDump();
                            responseBuilder.setStatusDump(statusDump.toProto());
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case METRIC_REGROUP:
                            numGroups = service.handleMetricRegroup(
                                    protoRequest.getSessionId(),
                                    protoRequest.getXStat(),
                                    protoRequest.getXMin(),
                                    protoRequest.getXMax(),
                                    protoRequest.getXIntervalSize(),
                                    protoRequest.getNoGutters()
                            );
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case METRIC_REGROUP_2D:
                            numGroups = service.handleMetricRegroup2D(
                                    protoRequest.getSessionId(),
                                    protoRequest.getXStat(),
                                    protoRequest.getXMin(),
                                    protoRequest.getXMax(),
                                    protoRequest.getXIntervalSize(),
                                    protoRequest.getYStat(),
                                    protoRequest.getYMin(),
                                    protoRequest.getYMax(),
                                    protoRequest.getYIntervalSize()
                            );
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case METRIC_FILTER:
                            numGroups = service.handleMetricFilter(
                                    protoRequest.getSessionId(),
                                    protoRequest.getXStat(),
                                    protoRequest.getXMin(),
                                    protoRequest.getXMax(),
                                    protoRequest.getNegate()
                            );
                            responseBuilder.setNumGroups(numGroups);
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case CREATE_DYNAMIC_METRIC:
                            service.handleCreateDynamicMetric(
                                    protoRequest.getSessionId(),
                                    protoRequest.getDynamicMetricName()
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case UPDATE_DYNAMIC_METRIC:
                            service.handleUpdateDynamicMetric(
                                    protoRequest.getSessionId(),
                                    protoRequest.getDynamicMetricName(),
                                    Ints.toArray(protoRequest.getDynamicMetricDeltasList())
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case CONDITIONAL_UPDATE_DYNAMIC_METRIC:
                            service.handleConditionalUpdateDynamicMetric(
                                    protoRequest.getSessionId(),
                                    protoRequest.getDynamicMetricName(),
                                    ImhotepDaemonMarshaller.marshalRegroupConditionMessageList(protoRequest.getConditionsList()),
                                    Ints.toArray(protoRequest.getDynamicMetricDeltasList())
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case GROUP_CONDITIONAL_UPDATE_DYNAMIC_METRIC:
                            service.handleGroupConditionalUpdateDynamicMetric(
                                    protoRequest.getSessionId(),
                                    protoRequest.getDynamicMetricName(),
                                    Ints.toArray(protoRequest.getGroupsList()),
                                    ImhotepDaemonMarshaller.marshalRegroupConditionMessageList(protoRequest.getConditionsList()),
                                    Ints.toArray(protoRequest.getDynamicMetricDeltasList())
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case OPTIMIZE_SESSION:
                            service.handleRebuildAndFilterIndexes(
                                    protoRequest.getSessionId(),
                                    getIntFields(protoRequest),
                                    getStringFields(protoRequest)
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case RESET_GROUPS:
                            service.handleResetGroups(
                                    protoRequest.getSessionId()
                            );
                            sendResponse(responseBuilder.build(), os);
                            break;
                        case MULTISPLIT_REGROUP:
                            numGroups = service.handleMultisplitRegroup(
                                    protoRequest.getSessionId(),
                                    ImhotepDaemonMarshaller.marshalGroupMultiRemapMessageList(protoRequest.getMultisplitRemapRuleList()),
                                    protoRequest.getErrorOnCollisions()
                            );
                            sendResponse(responseBuilder.setNumGroups(numGroups).build(), os);
                            break;
                        case EXPLODED_MULTISPLIT_REGROUP: {
                                final int numRules = protoRequest.getLength();
                                numGroups = service.handleMultisplitRegroup(protoRequest.getSessionId(), numRules, new UnmodifiableIterator<GroupMultiRemapRule>() {
                                    private int i = 0;

                                    @Override
                                    public boolean hasNext() {
                                        return i < numRules;
                                    }

                                    @Override
                                    public GroupMultiRemapRule next() {
                                        try {
                                            final GroupMultiRemapMessage message = ImhotepProtobufShipping.readGroupMultiRemapMessage(is);
                                            final GroupMultiRemapRule rule = ImhotepDaemonMarshaller.marshal(message);
                                            i++;
                                            return rule;
                                        } catch (IOException e) {
                                            throw Throwables.propagate(e);
                                        }
                                    }
                                },
                                protoRequest.getErrorOnCollisions());
                                sendResponse(responseBuilder.setNumGroups(numGroups).build(), os);
                                break;
                            }
                        case APPROXIMATE_TOP_TERMS:
                            topTerms = service.handleApproximateTopTerms(
                                    protoRequest.getSessionId(),
                                    protoRequest.getField(),
                                    protoRequest.getIsIntField(),
                                    protoRequest.getK()
                            );
                            sendResponse(responseBuilder.addAllTopTerms(ImhotepDaemonMarshaller.marshalTermCountList(topTerms)).build(), os);
                            break;
                        case SHUTDOWN:
                            if (protoRequest.hasSessionId() && "magicshutdownid".equals(protoRequest.getSessionId())) {
                                log.info("shutdown signal received, shutting down the JVM");
                                close(socket, is, os);
                                shutdown(true);
                            }
                            break;
                        default:
                            throw new IllegalArgumentException("unsupported request type: "+protoRequest.getRequestType());
                    }
                } catch (ImhotepOutOfMemoryException e) {
                    expireSession(protoRequest, e);
                    sendResponse(ImhotepResponse.newBuilder().setResponseCode(ImhotepResponse.ResponseCode.OUT_OF_MEMORY).build(), os);
                    log.warn("ImhotepOutOfMemoryException while servicing request", e);
                } catch (IOException e) {
                    sendResponse(newErrorResponse(e), os);
                    throw e;
                } catch (RuntimeException e) {
                    expireSession(protoRequest, e);
                    sendResponse(newErrorResponse(e), os);
                    throw e;
                } finally {
                    NDC.setMaxDepth(ndcDepth);
                    close(socket, is, os);
                }
            } catch (IOException e) {
                expireSession(protoRequest,e );
                if (e instanceof SocketException) {
                    log.warn("IOException while servicing request", e);
                } else {
                    log.error("IOException while servicing request", e);
                }
                throw new RuntimeException(e);
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
        boolean lazyLoadFiles = false;
        String cachingConfigFile = null;
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
            } else if (args[i].equals("--lazyLoadProps")) {
                ++i;
                cachingConfigFile = args[i];
                lazyLoadFiles = true;
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
                 lazyLoadFiles,
                 cachingConfigFile,
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
                            boolean lazyLoadFiles,
                            String cachingConfigFile,
                            String zkNodes,
                            String zkPath) throws IOException {
        ImhotepDaemon daemon = null;
        try {
            daemon = newImhotepDaemon(shardsDirectory,
                                      tempDirectory,
                                      port,
                                      memoryCapacityInMB,
                                      useCache,
                                      lazyLoadFiles,
                                      cachingConfigFile,
                                      zkNodes,
                                      zkPath);
            daemon.run();
        } finally {
            if (daemon != null) {
                daemon.shutdown(false);
            }
        }
    }

    static ImhotepDaemon newImhotepDaemon(String shardsDirectory,
                                          String shardTempDir,
                                          int port,
                                          long memoryCapacityInMB,
                                          boolean useCache,
                                          boolean lazyLoadFiles,
                                          String cachingConfigFile,
                                          String zkNodes,
                                          String zkPath) throws IOException {
        final ImhotepServiceCore localService;

        if (lazyLoadFiles) {
            CachedFile.initWithFile(cachingConfigFile, shardsDirectory.trim());
            localService =
                    new CachingLocalImhotepServiceCore(shardsDirectory, shardTempDir,
                                                       memoryCapacityInMB * 1024 * 1024, useCache,
                                                       new GenericFlamdexReaderSource(),
                                                       new LocalImhotepServiceConfig());
        } else {
            localService =
                    new LocalImhotepServiceCore(shardsDirectory, shardTempDir,
                                                memoryCapacityInMB * 1024 * 1024, useCache,
                                                new GenericFlamdexReaderSource(),
                                                new LocalImhotepServiceConfig());
        }
        final ServerSocket ss = new ServerSocket(port);
        final String myHostname = InetAddress.getLocalHost().getCanonicalHostName();
        return new ImhotepDaemon(ss, localService, zkNodes, zkPath, myHostname, port);
    }

    public ImhotepServiceCore getService() {
        return service;
    }

    public ServiceZooKeeperWrapper getZkWrapper() {
        return zkWrapper;
    }
}
