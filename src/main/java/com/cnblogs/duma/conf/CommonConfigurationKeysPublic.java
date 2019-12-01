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
}
