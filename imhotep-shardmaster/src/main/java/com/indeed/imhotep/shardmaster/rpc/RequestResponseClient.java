package com.indeed.imhotep.shardmaster.rpc;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.indeed.imhotep.client.Host;
import com.indeed.imhotep.shardmaster.ShardMaster;
import com.indeed.imhotep.shardmaster.protobuf.AssignedShard;
import com.indeed.imhotep.shardmaster.protobuf.HostAndPort;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterRequest;
import com.indeed.imhotep.shardmaster.protobuf.ShardMasterResponse;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.NoSuchElementException;

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

    private Iterable<ShardMasterResponse> sendAndReceive(final ShardMasterRequest request) throws IOException {
        final Socket socket = new Socket(serverHost.getHostname(), serverHost.getPort());
        ShardMasterMessageUtil.sendMessage(request, socket.getOutputStream());
        final InputStream socketInputStream = socket.getInputStream();
        return new Iterable<ShardMasterResponse>() {
            @Override
            public Iterator<ShardMasterResponse> iterator() {
                return new Iterator<ShardMasterResponse>() {
                    private ShardMasterResponse response;
                    @Override
                    public boolean hasNext() {
                        if (response != null) {
                            return true;
                        } else if (socket.isClosed()) {
                            return false;
                        } else {
                            try {
                                response = receiveResponse(request, socketInputStream);
                                return true;
                            } catch (final EOFException e) {
                                Closeables2.closeQuietly(socket, LOGGER);
                                return false;
                            } catch (final IOException e) {
                                Closeables2.closeQuietly(socket, LOGGER);
                                throw new IllegalStateException("Unexpected IO error while reading response", e);
                            }
                        }
                    }

                    @Override
                    public ShardMasterResponse next() {
                        if (response != null) {
                            final ShardMasterResponse result = response;
                            response = null;
                            return result;
                        }
                        throw new NoSuchElementException();
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException("Removal not supported");
                    }
                };
            }
        };
    }

    @Override
    public Iterable<AssignedShard> getAssignments(final String node) throws IOException {
        final ShardMasterRequest request = ShardMasterRequest.newBuilder()
                .setRequestType(ShardMasterRequest.RequestType.GET_ASSIGNMENT)
                .setNode(HostAndPort.newBuilder().setHost(node).build())
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
