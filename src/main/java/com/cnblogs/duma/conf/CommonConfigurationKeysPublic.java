package com.cnblogs.duma.conf;

/**
 * 包含配置相关的 key
 * @author duma
 */
public class CommonConfigurationKeysPublic {
    /** RPC连接超时 */
    public static final String  IPC_CLIENT_CONNECT_TIMEOUT_KEY =
            "ipc.client.connect.timeout";
    /** IPC_CLIENT_CONNECT_TIMEOUT_KEY 的默认值，20s */
    public static final int     IPC_CLIENT_CONNECT_TIMEOUT_DEFAULT = 20000;

    /** 连接最大的空闲时间 */
    public static final String  IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY =
            "ipc.client.connection.maxidletime";
    /** IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY 的默认值，10s */
    public static final int     IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT = 10000;

    /** rpc 客户端连接服务端超时的最大重试次数 */
    public static final String  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY =
            "ipc.client.connect.max.retries.on.timeouts";
    /** IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY 的默认值，45次 */
    public static final int  IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_DEFAULT = 45;

    public static final String  IPC_CLIENT_TCPNODELAY_KEY =
            "ipc.client.tcpnodelay";
    /** IPC_CLIENT_TCPNODELAY_KEY 的默认值，true */
    public static final boolean IPC_CLIENT_TCPNODELAY_DEFAULT = true;

    /** 是否允许 RPC 客户端向服务端发送 ping message */
    public static final String  IPC_CLIENT_PING_KEY = "ipc.client.ping";
    /** IPC_CLIENT_PING_KEY 的默认值，true */
    public static final boolean IPC_CLIENT_PING_DEFAULT = true;

    /** RPC 客户端向 RPC 服务端发送 ping message 的间隔 */
    public static final String  IPC_PING_INTERVAL_KEY = "ipc.ping.interval";
    /** IPC_PING_INTERVAL_KEY 的默认值，1min */
    public static final int     IPC_PING_INTERVAL_DEFAULT = 60000;

    /** Manis rpc 服务端 handler 线程个数 */
    public static final String  MANIS_HANDLER_COUNT_KEY = "dfs.namenode.handler.count";
    /** MANIS_HANDLER_COUNT_KEY 的默认值，10 */
    public static final int     MANIS_HANDLER_COUNT_DEFAULT = 10;

    /** Protobuf RPC Server的uri */
    public static final String  MANIS_RPC_PROTOBUF_KEY = "manis.rpc.uri.protobuf";

    /** RPC 服务端 socket 读线程的数量 */
    public static final String  IPC_SERVER_RPC_READ_THREADS_KEY =
            "ipc.server.read.threadpool.size";
    /** IPC_SERVER_RPC_READ_THREADS_KEY 的默认值，1个 */
    public static final int     IPC_SERVER_RPC_READ_THREADS_DEFAULT = 1;

    /** 队列中调用的数量 */
    public static final String  IPC_SERVER_HANDLER_QUEUE_SIZE_KEY =
            "ipc.server.handler.queue.size";
    /** IPC_SERVER_HANDLER_QUEUE_SIZE_KEY 的默认值，100个 */
    public static final int     IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT = 100;

    public static final String IPC_MAXIMUM_DATA_LENGTH =
            "ipc.maximum.data.length";
    /** IPC_MAXIMUM_DATA_LENGTH 的默认值，64MB */
    public static final int IPC_MAXIMUM_DATA_LENGTH_DEFAULT = 64 * 1024 * 1024;

    /** 响应大小大于该值将打 log */
    public static final String  IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY =
            "ipc.server.max.response.size";
    /** IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY 的默认值，1MB */
    public static final int     IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT =
            1024*1024;

    /** 每一个 socket reader pending 队列得大小 */
    public static final String IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY =
            "ipc.server.read.connection-queue.size";
    /** IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE 的大小，默认值：100 */
    public static final int IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT =
            100;
}
