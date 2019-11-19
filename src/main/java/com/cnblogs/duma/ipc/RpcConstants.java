package com.cnblogs.duma.ipc;

import java.nio.ByteBuffer;

public class RpcConstants {

    /** RPC 连接发送 header 的头四个字节 */
    public static final ByteBuffer HEADER = ByteBuffer.wrap("mrpc".getBytes());

    public static final byte CURRENT_VERSION = 1;

    public final static int CONNECTION_CONTEXT_CALL_ID = -3;

    public static final int INVALID_RETRY_COUNT = -1;

    public static final byte[] DUMMY_CLIENT_ID = new byte[0];
}
