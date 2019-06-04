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
 package com.indeed.imhotep.io;

import com.google.common.io.ByteStreams;
import com.google.protobuf.Message;
import com.indeed.imhotep.GroupStatsStreamReader;
import com.indeed.imhotep.api.GroupStatsIterator;
import com.indeed.imhotep.protobuf.GroupMultiRemapMessage;
import com.indeed.imhotep.protobuf.ImhotepRequest;
import com.indeed.imhotep.protobuf.ImhotepResponse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author jsgroth
 *
 * static utility methods for sending and receiving imhotep protobufs
 */
public final class ImhotepProtobufShipping {
    private ImhotepProtobufShipping() {}

    public static int getFullSizeInStream(final Message request) {
        return request.getSerializedSize() + 4;
    }

    public static void sendProtobufNoFlush(final Message request, final OutputStream os) throws IOException {
        os.write(Bytes.intToBytes(request.getSerializedSize()));
        request.writeTo(os);
    }

    public static void sendProtobuf(final Message request, final OutputStream os) throws IOException {
        sendProtobufNoFlush(request, os);
        os.flush();
    }

    public static void writeGroupStatsNoFlush(final GroupStatsIterator stats, final OutputStream os) throws IOException {
        final DataOutputStream stream = new DataOutputStream(os);
        while (stats.hasNext()) {
            stream.writeLong(stats.nextLong());
        }
    }

    public static GroupStatsIterator readGroupStatsIterator(final InputStream is, final int len, final boolean exhaust) {
        return new GroupStatsStreamReader(is, len, exhaust);
    }

    public static ImhotepRequest readRequest(final InputStream is) throws IOException {
        return ImhotepRequest.parseFrom(readPayloadStream(is));
    }

    public static GroupMultiRemapMessage readGroupMultiRemapMessage(final InputStream is) throws IOException {
        return GroupMultiRemapMessage.parseFrom(readPayloadStream(is));
    }

    public static ImhotepResponse readResponse(final InputStream is) throws IOException {
        return ImhotepResponse.parseFrom(readPayloadStream(is));
    }

    private static InputStream readPayloadStream(final InputStream is) throws IOException {
        final byte[] payloadLengthBytes = new byte[4];
        ByteStreams.readFully(is, payloadLengthBytes);
        final int payloadLength = Bytes.bytesToInt(payloadLengthBytes);

        return ByteStreams.limit(is, payloadLength);
    }


    public static byte[] runLengthEncode(final int[] values) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (final DataOutputStream daos = new DataOutputStream(baos)) {
            daos.writeInt(values.length);
            for (int i = 0; i < values.length; i++) {
                final int value = values[i];
                int count = 1;
                while (((i + 1) < values.length) && (values[i + 1] == value)) {
                    i += 1;
                    count += 1;
                }
                daos.writeInt(count);
                daos.writeInt(value);
            }
        } catch (final IOException e) {
            throw new RuntimeException("Writing to ByteArrayOutputStream should not throw IOException!", e);
        }
        return baos.toByteArray();
    }

    public static int[] runLengthDecodeIntArray(final byte[] bytes) {
        try (final DataInputStream dais = new DataInputStream(new ByteArrayInputStream(bytes))) {
            final int numValues = dais.readInt();
            int valueIndex = 0;
            final int[] values = new int[numValues];
            while (true) {
                final int count = dais.readInt();
                final int value = dais.readInt();

                for (int i = 0; i < count; i++) {
                    values[valueIndex] = value;
                    valueIndex += 1;
                }

                if (valueIndex == numValues) {
                    return values;
                }
            }
        } catch (final IOException e) {
            throw new RuntimeException("Writing from ByteArrayInputStream should not throw IOException!", e);
        }
    }
}
