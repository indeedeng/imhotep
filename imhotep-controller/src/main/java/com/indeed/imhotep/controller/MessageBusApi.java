package com.indeed.imhotep.controller;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;

public class MessageBusApi {
    
    private final long clientPtr;
    private static final Unsafe unsafe;
    private static final int ARRAY_BYTE_BASE_OFFSET;
    private static final int ARRAY_BYTE_INDEX_SCALE;
    
    static {
        try {
            final Field f;
            f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            unsafe = (Unsafe) f.get(null);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        ARRAY_BYTE_BASE_OFFSET = unsafe.ARRAY_BYTE_BASE_OFFSET;
        ARRAY_BYTE_INDEX_SCALE = unsafe.ARRAY_BYTE_INDEX_SCALE;

        native_init();
    }
    
    
    public MessageBusApi(String hostname, int port, String sourceId) {

        this.clientPtr = message_bus_client_new();
        
        final long hostnamePtr = copyStringToNative(hostname);
        final long sourceIdPtr = copyStringToNative(sourceId);

        final byte rc;
        rc = message_bus_client_connect(this.clientPtr, hostnamePtr, port, -1, sourceIdPtr);
        
        unsafe.freeMemory(sourceIdPtr);
        unsafe.freeMemory(hostnamePtr);
        
        checkReturnCode(rc, "Error connecting to message server.");
    }
    
    private void checkReturnCode(byte returnCode, String errorMsg) {
        if (returnCode < 0) {
            throw new RuntimeException(errorMsg 
                                        + " status: " 
                                        +  message_bus_client_status(this.clientPtr)
                                        + " reason: "
                                        + "foo");
        }
    }
    
    private long copyStringToNative(String str) {
        final byte[] strBytes = str.getBytes(StandardCharsets.ISO_8859_1);
        return copyBufferToNative(strBytes);
    }
    
    private long copyBufferToNative(byte[] buffer) {
        // + 1 for zero termination + 7 to round up
        final long len = (buffer.length + 1 + 7) & ~7L;  // round up to 8 byte value
        final long nativePtr = unsafe.allocateMemory(len);
        unsafe.copyMemory(buffer, ARRAY_BYTE_BASE_OFFSET, null, nativePtr, len);
        unsafe.putByte(nativePtr + buffer.length, (byte)0);  // zero terminate the buffer
        return nativePtr;
    }

    private String copyStringFromNative(long ptr, int len) {
        final byte[] strBytes = copyBufferFromNative(ptr, len);
        return new String(strBytes, StandardCharsets.ISO_8859_1);
    }

    private byte[] copyBufferFromNative(long ptr, int len) {
        final byte[] buffer = new byte[len];
        unsafe.copyMemory(null, ptr, buffer, ARRAY_BYTE_BASE_OFFSET, len);
        unsafe.freeMemory(ptr);
        return buffer;
    }

    public synchronized void subscribe(String topicPrefix) {
        final long topicPrefixPtr = copyStringToNative(topicPrefix);

        final byte rc;
        rc = message_bus_client_subscribe(this.clientPtr, topicPrefixPtr);
        
        unsafe.freeMemory(topicPrefixPtr);
        
        checkReturnCode(rc, "Error subscribing to topic " + topicPrefix + ".");
    }
    
    public synchronized void unsubscribe(String topicPrefix) {
        final long topicPrefixPtr = copyStringToNative(topicPrefix);

        final byte rc;
        rc = message_bus_client_unsubscribe(this.clientPtr, topicPrefixPtr);
        
        unsafe.freeMemory(topicPrefixPtr);
        
        checkReturnCode(rc, "Error unsubscribing to topic " + topicPrefix + ".");
    }
    
    public synchronized void publishMessage(String topic, byte[] data) {
        final long topicPtr = copyStringToNative(topic);
        final long bufferPtr = copyBufferToNative(data);

        final byte rc;
        rc = message_bus_client_publish(this.clientPtr, topicPtr, bufferPtr, data.length);
        
        unsafe.freeMemory(topicPtr);
        
        checkReturnCode(rc, "Error sending message to topic " + topic + ".");
    }
    
    /* waits until bus receives a message */
    public Message waitForMessage() {
        final NativeMessageData data = wait_for_message(this.clientPtr);

        final Message result = new Message();
        result.topic = copyStringFromNative(data.topic, data.topicLen);
        result.sourceId = copyStringFromNative(data.sourceId, data.sourceIdLen);
        result.data = copyBufferFromNative(data.data, data.dataLen);
        return result;
    }

    //  Return last received reason, returns a pointer to the string on the native heap
    private native static void native_init();

    //  Create a new message_bus_client, return the reference if successful, or NULL
    //  if construction failed due to lack of available memory.
    private native long message_bus_client_new ();
    
    //  Destroy the message_bus_client and free all memory used by the object.
    private native void message_bus_client_destroy (long self_p);
    
    //  Return actor, when caller wants to work with multiple actors and/or
    //  input sockets asynchronously.
    private native long message_bus_client_actor (long self);
    
    //  Return message pipe for asynchronous message I/O. In the high-volume case,
    //  we send methods and get replies to the actor, in a synchronous manner, and
    //  we send/recv high volume message data to a second pipe, the msgpipe. In
    //  the low-volume case we can do everything over the actor pipe, if traffic
    //  is never ambiguous.
    private native long message_bus_client_msgpipe (long self);
    private native NativeMessageData wait_for_message (long self);
    
    //  Return true if client is currently connected, else false. Note that the
    //  client will automatically re-connect if the server dies and restarts after
    //  a successful first connection.
    private native boolean message_bus_client_connected (long self);
    
    //  Connect to server endpoint, with specified timeout in msecs (zero means wait    
    //  forever). Connect succeeds if connection is successful.                         
    //  Returns >= 0 if successful, -1 if interrupted.
    private native byte message_bus_client_connect (long self,
                                                    long hostname_buffer_p, 
                                                    int port, 
                                                    int timeout, 
                                                    long source_id_buffer_p);
    
    //  Subscribe to a topic on the server, will subrscibe to all topics that match the 
    //  passed in prefix                                                                
    //  Returns >= 0 if successful, -1 if interrupted.
    private native byte message_bus_client_subscribe (long self, long prefix_buffer_p);
    
    //  Unsubscribe to a topic                                                          
    //  Returns >= 0 if successful, -1 if interrupted.
    private native byte message_bus_client_unsubscribe (long self, long prefix_buffer_p);
    
    //  Publish a message on a topic                                                    
    //  Returns >= 0 if successful, -1 if interrupted.
    private native byte message_bus_client_publish (long self, 
                                                    long topic_buffer_p, 
                                                    long data_buffer_p,
                                                    int buffer_len);
    
    //  Return last received status
    private native byte message_bus_client_status (long self);
    
    //  Return last received reason, returns a pointer to the string on the native heap
    private native long message_bus_client_reason (long self);

    public static class Message {
        public String topic;
        public String sourceId;
        public byte[] data;
    }

    private static class NativeMessageData {
        public long topic;
        public int topicLen;
        public long sourceId;
        public int sourceIdLen;
        public long data;
        public int dataLen;

        public NativeMessageData(long topic,
                                 int topicLen,
                                 long sourceId,
                                 int sourceIdLen,
                                 long data,
                                 int dataLen) {
            this.topic = topic;
            this.topicLen = topicLen;
            this.sourceId = sourceId;
            this.sourceIdLen = sourceIdLen;
            this.data = data;
            this.dataLen = dataLen;
        }
    }
}