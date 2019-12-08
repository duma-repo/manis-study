package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * @author duma
 */
public abstract class Server {
    public static final Log LOG = LogFactory.getLog(Server.class);
    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThreads;

    /** callQueue 队列的大小 */
    private int maxQueueSize;
    /** 响应 buffer 的上限 */
    private int maxRespSize;
    /** 客户端信息长度的上限 */
    private int maxDataLength;
    private boolean tcpNoDelay;
    /** pendingConnections 队列的大小 */
    private int readerPendingConnectionQueue;

    private BlockingQueue<Call> callQueue;

    volatile private boolean running = true;

    private ConnectionManager connectionManager;
    private Listener listener;
    private Responder responder;
    private Handler[] handlers = null;

    private Configuration conf;

    static class RpcKindMapValue {
        final Class<? extends Writable> rpcRequestWrapperClass;
        final RPC.RpcInvoker rpcInvoker;

        RpcKindMapValue(Class<? extends Writable> rpcRequestWrapperClass,
                        RPC.RpcInvoker rpcInvoker) {
            this.rpcRequestWrapperClass = rpcRequestWrapperClass;
            this.rpcInvoker = rpcInvoker;
        }
    }
    static Map<RPC.RpcKind, RpcKindMapValue> rpcKindMap = new HashMap<>();

    /**
     * 将RpcKind对象和RpcKindMapValue对象组成的键值对写入 rpcKindMap 集合
     * @param rpcKind RPC.RpcKind 对象
     * @param rpcRequestWrapperClass 调用请求的封装类的Class对象
     * @param rpcInvoker 服务端方法调用类对象
     */
    static void registerProtocolEngine(RPC.RpcKind rpcKind,
                                       Class<? extends Writable> rpcRequestWrapperClass,
                                       RPC.RpcInvoker rpcInvoker) {
        RpcKindMapValue rpcKindMapValue =
                new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker);
        RpcKindMapValue old = rpcKindMap.put(rpcKind, rpcKindMapValue);
        if (old != null) {
            rpcKindMap.put(rpcKind, old);
            throw new IllegalArgumentException("ReRegistration of rpcKind: " +  rpcKind);
        }
        LOG.debug("rpcKind=" + rpcKind +
                ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
                ", rpcInvoker=" + rpcInvoker);
    }

    /**
     * Server 类构造方法
     * @param bindAddress 服务端地址
     * @param port 服务端端口
     * @param numHandlers Handler 线程的数量
     * @param numReaders Reader 线程的数量
     * @param queueSizePerHandler 每个 Handler 期望的消息队列大小，再根据 numHandlers 可以得出队列总大小
     * @param conf 配置
     * @throws IOException
     */
    protected Server(String bindAddress, int port,
                     int numHandlers, int numReaders, int queueSizePerHandler,
                     Configuration conf) throws IOException {
        this.conf = conf;
        this.bindAddress = bindAddress;
        this.port = port;
        this.handlerCount = numHandlers;

        if (numReaders != -1) {
            this.readThreads = numReaders;
        } else {
            this.readThreads = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_THREADS_DEFAULT);
        }
        if (queueSizePerHandler != -1) {
            this.maxQueueSize = numHandlers * queueSizePerHandler;
        } else {
            this.maxQueueSize = numHandlers * conf.getInt(
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_KEY,
                    CommonConfigurationKeysPublic.IPC_SERVER_HANDLER_QUEUE_SIZE_DEFAULT);
        }
        this.maxDataLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH,
                CommonConfigurationKeysPublic.IPC_MAXIMUM_DATA_LENGTH_DEFAULT);
        this.maxRespSize = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_MAX_RESPONSE_SIZE_DEFAULT);
        this.readerPendingConnectionQueue = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_RPC_READ_CONNECTION_QUEUE_SIZE_DEFAULT);
        this.callQueue = new LinkedBlockingDeque<>(maxQueueSize);

        // 创建 listener
        this.listener = new Listener();
        this.connectionManager = new ConnectionManager();

        this.tcpNoDelay = conf.getBoolean(
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_KEY,
                CommonConfigurationKeysPublic.IPC_CLIENT_TCPNODELAY_DEFAULT);

        // 创建 responder
        this.responder = new Responder();
    }

    /**
     * 启动服务
     */
    public synchronized void start() {
        listener.start();
        responder.start();
        handlers = new Handler[handlerCount];

        for (int i = 0; i < handlerCount; i++) {
            handlers[i] = new Handler(i);
            handlers[i].start();
        }
    }

    /**
     * 等待服务端停止
     * @throws InterruptedException
     */
    public synchronized void join() throws InterruptedException {
        while (running) {
            wait();
        }
    }

    public static class Call {
        private final int callId;
        private final int retryCount;
        /** 客户端发来的序列化的 RPC 请求 */
        private final Writable rpcRequest;
        private final Connection connection;
        private long timestamp;

        /** 本次调用的响应信息 */
        private ByteBuffer response;
        private final RPC.RpcKind rpcKind;
        private final byte[] clientId;

        public Call(int id, int retryCount, Writable rpcRequest, Connection connection) {
            this(id, retryCount, rpcRequest, connection,
                    RPC.RpcKind.RPC_BUILTIN, RpcConstants.DUMMY_CLIENT_ID);
        }

        public Call(int id, int retryCount, Writable rpcRequest,
                    Connection connection, RPC.RpcKind rpcKind, byte[] clientId) {
            this.callId = id;
            this.retryCount = retryCount;
            this.rpcRequest = rpcRequest;
            this.connection = connection;
            this.timestamp = System.currentTimeMillis();
            this.response = null;
            this.rpcKind = rpcKind;
            this.clientId = clientId;
        }

        public void setResponse(ByteBuffer response) {
            this.response = response;
        }

        @Override
        public String toString() {
            return rpcRequest + " from " + connection + " Call#" + callId +
                    " Retry#" + retryCount;
        }
    }

    /**
     * socket 监听线程
     */
    private class Listener extends Thread {

    }

    /**
     * 处理队列中的调用请求
     */
    private class Handler extends Thread {
        Handler(int instanceNumber) {
            this.setDaemon(true);
            this.setName("IPC Server handler " + instanceNumber + " on " + port);
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread {

    }

    private class Connection {

    }

    private class ConnectionManager {

    }
}
