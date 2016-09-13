package com.indeed.imhotep.shardmaster.rpc;

import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.protobuf.HostAndPort;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClient implements ShardMaster {
    private final Host serverHost;

    public RequestResponseClient(final Host serverHost) {
        this.serverHost = serverHost;
    }

    private ShardMasterResponse sendAndReceive(final ShardMasterRequest request) throws IOException {
        try (Socket socket = new Socket(serverHost.getHostname(), serverHost.getPort())) {
            ShardMasterMessageUtil.sendMessage(request, socket.getOutputStream());
            final ShardMasterResponse response = ShardMasterMessageUtil.receiveResponse(socket.getInputStream());
            switch (response.getResponseCode()) {
                case OK:
                    return response;
                case ERROR:
                    throw new IOException("Received error for request " + request + ": " + response.getErrorMessage());
                default:
                    throw new IllegalStateException("Received unexpected response code " + response.getResponseCode());
            }
        }
    }

    @Override
    public List<AssignedShard> getAssignments(final String node) throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_ASSIGNMENT)
                .setNode(HostAndPort.newBuilder().setHost(node).build())
                .build();

        final ShardMasterResponse response = sendAndReceive(request);
        return response.getAssignedShardsList();
    }
}
