package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.protobuf.HostAndPort;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClient implements ShardMaster {
    private static final Logger LOGGER = Logger.getLogger(RequestResponseClient.class);
    private final Host serverHost;

    public RequestResponseClient(final Host serverHost) {
        this.serverHost = serverHost;
    }

    private ShardMasterResponse receiveResponse(final ShardMasterRequest request, final InputStream is) throws IOException {
        final ShardMasterResponse response = ShardMasterMessageUtil.receiveResponse(is);
        switch (response.getResponseCode()) {
            case OK:
                return response;
            case ERROR:
                throw new IOException("Received error for request " + request + ": " + response.getErrorMessage());
            default:
                throw new IllegalStateException("Received unexpected response code " + response.getResponseCode());
        }
    }

    private List<ShardMasterResponse> sendAndReceive(final ShardMasterRequest request) throws IOException {
        try (Socket socket = new Socket(serverHost.getHostname(), serverHost.getPort())) {
            ShardMasterMessageUtil.sendMessage(request, socket.getOutputStream());
            try (InputStream socketInputStream = socket.getInputStream()) {
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
    public Iterable<AssignedShard> getAssignments(final Host node) throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_ASSIGNMENT)
                .setNode(HostAndPort.newBuilder().setHost(node.getHostname()).setPort(node.getPort()).build())
                .build();

        return FluentIterable.from(sendAndReceive(request))
                .transformAndConcat(new Function<ShardMasterResponse, Iterable<? extends AssignedShard>>() {
                    @Override
                    public Iterable<? extends AssignedShard> apply(final ShardMasterResponse response) {
                        return response.getAssignedShardsList();
                    }
                });
    }
}
