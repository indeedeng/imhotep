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

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.indeed.imhotep.protobuf.ShardMasterRequest;
import com.indeed.imhotep.protobuf.ShardMasterResponse;
import com.indeed.util.core.io.Closeables2;
import org.apache.log4j.Logger;

import javax.annotation.WillCloseWhenClosed;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * @author kenh
 */

public class RequestResponseServer implements Closeable {
    private static final Logger LOGGER = Logger.getLogger(RequestResponseServer.class);

    private final RequestHandler requestHandler;
    private final ExecutorService requestHandlerExecutor;
    private final ServerSocket serverSocket;

    public RequestResponseServer(@WillCloseWhenClosed final ServerSocket socket, final RequestHandler requestHandler, final int numThreads) throws IOException {
        this.requestHandler = requestHandler;
        requestHandlerExecutor = ShardMasterExecutors.newBlockingFixedThreadPool(numThreads);
        serverSocket = socket;
    }

    public int getActualPort() {
        if (serverSocket.isClosed()) {
            return 0;
        } else {
            return serverSocket.getLocalPort();
        }
    }

    @Override
    public void close() throws IOException {
        LOGGER.info("closing socket");
        serverSocket.close();
        requestHandlerExecutor.shutdown();
    }

    public void run() throws IOException {
        while (!serverSocket.isClosed()) {
            try {
                final Socket socket;
                try {
                    socket = serverSocket.accept();
                } catch (final SocketException e) {
                    if ("Socket closed".equals(e.getMessage())) {
                        LOGGER.warn("Shutting down due to server socket closed");
                        break;
                    }
                    throw e;
                }

                socket.setTcpNoDelay(true); // disable nagle
                socket.setSoTimeout(60000);

                requestHandlerExecutor.submit(() -> {
                    final ShardMasterRequest request;
                    try {
                        try {
                            request = ShardMasterMessageUtil.receiveRequest(socket.getInputStream());
                        } catch (final IOException e) {
                            if(e instanceof InvalidProtocolBufferException) {
                                LOGGER.debug("Error while reading request: " + e.getMessage());
                            } else {
                                LOGGER.error("Error while reading request", e);
                            }
                            Closeables2.closeQuietly(socket, LOGGER);
                            return;
                        }

                        Iterable<ShardMasterResponse> responses;
                        try {
                            responses = requestHandler.handleRequest(request);
                        } catch (final Throwable e) {
                            LOGGER.error("Failed to handle request from " + request.getNode().getHost()
                                    + ": "+ request, e);
                            responses = Collections.singletonList(ShardMasterResponse.newBuilder()
                                    .setResponseCode(ShardMasterResponse.ResponseCode.ERROR)
                                    .setErrorMessage(Throwables.getStackTraceAsString(e))
                                    .build());
                        }

                        try {
                            final OutputStream socketStream = socket.getOutputStream();
                            for (final ShardMasterResponse response : responses) {
                                ShardMasterMessageUtil.sendMessageNoFlush(response, socketStream);
                            }
                            socketStream.flush();
                        } catch (final IOException e) {
                            LOGGER.error("Error while responding to request from "
                                    + request.getNode().getHost() + ": " + request, e);
                        }
                    } finally {
                        Closeables2.closeQuietly(socket, LOGGER);
                    }
                });
            } catch (final Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
