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

package com.indeed.imhotep.shardmasterrpc;

import com.indeed.imhotep.DatasetInfo;
import com.indeed.imhotep.Shard;
import com.indeed.imhotep.ShardInfo;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.protobuf.DatasetInfoMessage;
import com.indeed.imhotep.protobuf.DatasetShardsMessage;
import com.indeed.imhotep.protobuf.HostAndPort;
import com.indeed.imhotep.protobuf.ShardMasterRequest;
import com.indeed.imhotep.protobuf.ShardMasterResponse;
import com.indeed.imhotep.protobuf.ShardMessage;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author kenh
 */

public class RequestResponseClient implements ShardMaster {
    private static final Logger log = Logger.getLogger(RequestResponseClient.class);

    private static final int SOCKET_READ_TIMEOUT_MILLIS = 30000;

    private static final int SOCKET_CONNECTING_TIMEOUT_MILLIS = 30000;

    private final Random random = new Random();
    private final List<Host> serverHosts;
    private static String currentHostName;
    static {
        try {
            currentHostName = InetAddress.getLocalHost().getHostName();
        }
        catch (final UnknownHostException ex) {
            currentHostName = "(unknown)";
        }
    }

    public RequestResponseClient(final List<Host> serverHosts) {
        if(serverHosts == null || serverHosts.isEmpty()) {
            throw new IllegalArgumentException("At least one server is required for communicating with ShardMaster");
        }
        this.serverHosts = serverHosts;
    }

    private ShardMasterResponse receiveResponse(final ShardMasterRequest request, final InputStream is) throws IOException {
        final ShardMasterResponse response = ShardMasterMessageUtil.receiveResponse(is);
        switch (response.getResponseCode()) {
            case OK:
                return response;
            case ERROR:
                throw new IOException("Received error from " + serverHosts + " for request " + request + ": " + response.getErrorMessage());
            default:
                throw new IllegalStateException("Received unexpected response code " + response.getResponseCode());
        }
    }

    private List<ShardMasterResponse> sendAndReceiveWithRetries(final ShardMasterRequest request) throws IOException {
        final int firstServerToTry = random.nextInt(serverHosts.size());
        IOException lastError = null;
        for(int i = 0; i < serverHosts.size(); i++) {
            final Host serverToTry = serverHosts.get((firstServerToTry + i) % serverHosts.size());
            try {
                return sendAndReceive(request, serverToTry);
            } catch (IOException e) {
                if(i != serverHosts.size() - 1) {
                    log.info("IO failure with ShardMaster request " + request.getRequestType() +
                            " to " + serverToTry + ", retrying with another server. Error: " + e.toString());
                }
                lastError = e;
            }
        }
        if(lastError == null) {
            // this should never happen but it makes IntelliJ happy
            throw new IllegalStateException("At least one server is required for communicating with ShardMaster");
        }
        throw lastError;
    }

    private List<ShardMasterResponse> sendAndReceive(final ShardMasterRequest request, final Host serverHost) throws IOException {
        try (final Socket socket = new Socket()) {
            final SocketAddress endpoint = new InetSocketAddress(serverHost.getHostname(), serverHost.getPort());
            socket.connect(endpoint, SOCKET_CONNECTING_TIMEOUT_MILLIS);
            socket.setSoTimeout(SOCKET_READ_TIMEOUT_MILLIS);
            ShardMasterMessageUtil.sendMessage(request, socket.getOutputStream());
            try (final InputStream socketInputStream = socket.getInputStream()) {
                final List<ShardMasterResponse> responses = new ArrayList<>();
                while (true) {
                    try {
                        responses.add(receiveResponse(request, socketInputStream));
                    } catch (final EOFException e) {
                        return responses;
                    }
                }
            }
        }
    }

    @Override
    public List<DatasetInfo> getDatasetMetadata() throws IOException{
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_DATASET_METADATA)
                .setNode(HostAndPort.newBuilder().setHost(currentHostName).build())
                .build();
        final List<ShardMasterResponse> shardMasterResponses = sendAndReceiveWithRetries(request);

        final List<DatasetInfo> toReturn = new ArrayList<>();
        for(final ShardMasterResponse response: shardMasterResponses){
            for(final DatasetInfoMessage metadata: response.getMetadataList()) {
                toReturn.add(DatasetInfo.fromProto(metadata));
            }
        }
        return toReturn;
    }

    @Override
    public List<Shard> getShardsInTime(final String dataset, final long start, final long end) throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_SHARD_LIST_FOR_TIME)
                .setStartTime(start)
                .setEndTime(end)
                .setDataset(dataset)
                .setNode(HostAndPort.newBuilder().setHost(currentHostName).build())
                .build();
        final List<Shard> toReturn = new ArrayList<>();
        final List<ShardMasterResponse> shardMasterResponses = sendAndReceiveWithRetries(request);
        for(final ShardMasterResponse response: shardMasterResponses){
            final List<ShardMessage> shardsInTimeList = response.getShardsInTimeList();
            for(final ShardMessage message: shardsInTimeList) {
                toReturn.add(Shard.fromShardMessage(message));
            }
        }
        return toReturn;
    }

    @Override
    public Map<String, Collection<ShardInfo>> getShardList() throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_SHARD_LIST)
                .setNode(HostAndPort.newBuilder().setHost(currentHostName).build())
                .build();

        final List<DatasetShardsMessage> datasetMessages = sendAndReceiveWithRetries(request).stream()
                .map(ShardMasterResponse::getAllShardsList)
                .flatMap(List::stream).collect(Collectors.toList());

        final Map<String, Collection<ShardInfo>> toReturn = new HashMap<>();

        for(final DatasetShardsMessage message: datasetMessages) {
            toReturn.put(message.getDataset(), message.getShardsList().stream().map(ShardInfo::fromProto).collect(Collectors.toList()));
        }

        return toReturn;
    }

    @Override
    public void refreshFieldsForDataset(final String dataset) throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.REFRESH_FIELDS_FOR_DATASET)
                .setNode(HostAndPort.newBuilder().setHost(currentHostName).build())
                .setDatasetToRefresh(dataset).build();

        // this request has to go to all the hosts
        for(Host serverHost : serverHosts) {
            sendAndReceive(request, serverHost);
        }
    }
}
