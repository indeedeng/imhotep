package com.indeed.imhotep.shardmanager.rpc;

import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmanager.ShardManager;
import com.indeed.imhotep.shardmanager.protobuf.AssignedShard;
import com.indeed.imhotep.shardmanager.protobuf.HostAndPort;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerRequest;
import com.indeed.imhotep.shardmanager.protobuf.ShardManagerResponse;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

/**
 * @author kenh
 */

public class RequestResponseClient implements ShardManager {
    private final Host serverHost;

    public RequestResponseClient(final Host serverHost) {
        this.serverHost = serverHost;
    }

    private ShardManagerResponse sendAndReceive(final ShardManagerRequest request) throws IOException {
        try (Socket socket = new Socket(serverHost.getHostname(), serverHost.getPort())) {
            ShardManagerMessageUtil.sendMessage(request, socket.getOutputStream());
            final ShardManagerResponse response = ShardManagerMessageUtil.receiveResponse(socket.getInputStream());
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
        final ShardManagerResponse response = sendAndReceive(ShardManagerRequest.newBuilder()
                .setRequestType(ShardManagerRequest.RequestType.GET_ASSIGNMENT)
                .setNode(HostAndPort.newBuilder().setHost(node).build())
                .build());

        return response.getAssignedShardsList();
    }
}
