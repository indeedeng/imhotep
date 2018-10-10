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

import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Message;
import com.indeed.imhotep.io.Bytes;
import com.indeed.imhotep.protobuf.ShardMasterRequest;
import com.indeed.imhotep.protobuf.ShardMasterResponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author kenh
 */

class ShardMasterMessageUtil {
    private ShardMasterMessageUtil() {
    }

    static void sendMessageNoFlush(final Message message, final OutputStream os) throws IOException {
        os.write(Ints.toByteArray(message.getSerializedSize()));
        message.writeTo(os);
    }

    static void sendMessage(final Message message, final OutputStream os) throws IOException {
        sendMessageNoFlush(message, os);
        os.flush();
    }

    static ShardMasterRequest receiveRequest(final InputStream is) throws IOException {
        return ShardMasterRequest.parseFrom(readPayloadStream(is));
    }

    static ShardMasterResponse receiveResponse(final InputStream is) throws IOException {
        final CodedInputStream codedInputStream = CodedInputStream.newInstance(readPayloadStream(is));
        // Allow for responses over 64MB for dataset metadata with full shard list RPC call
        codedInputStream.setSizeLimit(Integer.MAX_VALUE);
        return ShardMasterResponse.parseFrom(codedInputStream);
    }

    private static InputStream readPayloadStream(final InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);
        return ByteStreams.limit(is, payloadLength);
    }
}
