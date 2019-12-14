package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

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

    private void closeConnection(Connection conn) {
        connectionManager.close(conn);
    }

    /**
     * 封装 socket 与 address 绑定的代码，为了在这层做异常的处理
     * @param socket 服务端 socket
     * @param address 需要绑定的地址
     * @throws IOException
     */
    public static void bind(ServerSocket socket, InetSocketAddress address,
                            int backlog) throws IOException {
        try {
            socket.bind(address, backlog);
        } catch (SocketException se) {
            throw new IOException("Failed on local exception: "
                    + se
                    + "; bind address: " + address, se);
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
        private ServerSocketChannel acceptChannel;
        private Selector selector;
        private Reader[] readers;
        private int currentReader = 0;
        private InetSocketAddress address;
        private int backlogLength = conf.getInt(
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_KEY,
                CommonConfigurationKeysPublic.IPC_SERVER_LISTEN_QUEUE_SIZE_DEFAULT);

        public Listener() throws IOException {
            this.address = new InetSocketAddress(bindAddress, port);
            // 创建服务端 socket，并设置为非阻塞
            this.acceptChannel = ServerSocketChannel.open();
            this.acceptChannel.configureBlocking(false);

            // 将服务端 socket 绑定到主机和端口
            bind(acceptChannel.socket(), address, backlogLength);
            // 创建 selector
            this.selector = Selector.open();
            // 注册 select ACCEPT 事件
            acceptChannel.register(selector, SelectionKey.OP_ACCEPT);

            // 创建 reader 线程并启动
            this.readers = new Reader[readThreads];
            for (int i = 0; i < readThreads; i++) {
                Reader reader = new Reader("Socket Reader #" + (i + 1) + " for port " + port);
                this.readers[i] = reader;
                reader.start();
            }

            this.setName("IPC Server listener on " + port);
            // 设置为 daemon，它的子线程也是 daemon
            this.setDaemon(true);
        }

        private class Reader extends Thread {
            private final BlockingQueue<Connection> pendingConnections;
            private final Selector readSelector;

            Reader(String name) throws IOException {
                super(name);
                this.pendingConnections =
                        new LinkedBlockingDeque<>(readerPendingConnectionQueue);
                readSelector = Selector.open();
            }

            /**
             * 生产者将 connection 入队列，唤醒 readSelector
             * @param conn Connection 对象
             * @throws InterruptedException
             */
            void addConnection(Connection conn) throws InterruptedException {
                pendingConnections.put(conn);
                readSelector.wakeup();
            }
        }

        @Override
        public void run() {
            LOG.info("Listener thread " + Thread.currentThread().getName() + ": starting");
            connectionManager.startIdleScan();
            while (running) {
                SelectionKey key = null;
                try {
                    selector.select();
                    Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
                    while (iterator.hasNext()) {
                        key = iterator.next();
                        iterator.remove();
                        if (key.isValid()) {
                            if (key.isAcceptable()) {
                                System.out.println("client accept");
                                doAccept(key);
                            }
                        }
                        key = null;
                    }
                } catch (OutOfMemoryError oom) {
                    // out of memory 时，需要关闭所有连接
                    LOG.warn("Out of Memory in server select", oom);
                    closeCurrentConnection(key);
                    connectionManager.closeIdle(true);
                    try { Thread.sleep(60000); } catch (InterruptedException e) {}
                } catch (Exception e) {
                    closeCurrentConnection(key);
                }
            }
            // 需要停止，退出循环
            LOG.info("Stopping " + Thread.currentThread().getName());

            try {
                acceptChannel.close();
                selector.close();
            } catch (IOException e) {
                LOG.warn("Ignoring listener close exception", e);
            }

            acceptChannel = null;
            selector = null;

            connectionManager.stopIdleScan();
            connectionManager.closeAll();
        }

        private void closeCurrentConnection(SelectionKey key) {
            if (key != null) {
                Connection conn = (Connection) key.attachment();
                if (conn != null) {
                    closeConnection(conn);
                    conn = null;
                }
            }
        }

        void doAccept(SelectionKey key) throws IOException, InterruptedException {
            ServerSocketChannel server = (ServerSocketChannel) key.channel();
            SocketChannel channel;
            while ((channel = server.accept()) != null) {
                channel.configureBlocking(false);
                channel.socket().setTcpNoDelay(tcpNoDelay);
                channel.socket().setKeepAlive(true);

                Connection conn = connectionManager.register(channel);
                // 关闭当前连接时可以去到 Connection 对象
                key.attach(conn);

                Reader reader = getReader();
                reader.addConnection(conn);
            }
        }

        Reader getReader() {
            currentReader = (currentReader + 1) % readers.length;
            return readers[currentReader];
        }
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
        private SocketChannel channel;
        private volatile long lastContact;

        private Socket socket;
        private InetAddress remoteAddr;
        private String hostAddress;
        private int remotePort;

        private volatile int rpcCount;

        Connection(SocketChannel channel, long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
            this.remoteAddr = socket.getInetAddress();
            this.hostAddress = remoteAddr.getHostAddress();
            this.remotePort = socket.getPort();
        }

        private void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }

        private long getLastContact() {
            return lastContact;
        }

        private synchronized void close() {

        }

        private boolean isIdle() {
            return rpcCount == 0;
        }

        private void incRpcCount() {
            rpcCount++;
        }

        private void decRpcCount() {
            rpcCount--;
        }
    }

    private class ConnectionManager {
        /** 当前网络连接的数量 */
        private final AtomicInteger count = new AtomicInteger();
        /** 存放所有的网络连接 */
        private Set<Connection> connections;

        /** 扫描空闲连接的定时器 */
        final private Timer idleScanTimer;
        /** 定时器延迟多长时间后执行，单位：毫秒 */
        final private int idleScanInterval;
        /** 需要关闭控线连接的阈值，当前连接数超过该值便关闭空闲的连接 */
        final private int idleScanThreshold;
        /** 允许网络连接最大空闲的时间，超过该时间有可能被回收 */
        final private int maxIdleTime;
        /** 每一次扫描关闭最大的连接数量 */
        final private int maxIdleToClose;

        ConnectionManager() {
            this.idleScanTimer = new Timer(
                    "IPC Server idle connection scanner for port " + port, true);
            this.idleScanThreshold = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_IDLETHRESHOLD_DEFAULT);
            this.idleScanInterval = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_IDLESCANINTERVAL_DEFAULT);
            this.maxIdleTime = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_CONNECTION_MAXIDLETIME_DEFAULT);
            this.maxIdleToClose = conf.getInt(
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_KEY,
                    CommonConfigurationKeysPublic.IPC_CLIENT_KILL_MAX_DEFAULT);
            this.connections = Collections.newSetFromMap(
                    new ConcurrentHashMap<Connection, Boolean>(maxQueueSize));
        }

        private boolean add(Connection connection) {
            boolean added = connections.add(connection);
            if (added) {
                count.getAndIncrement();
            }
            return added;
        }

        private boolean remove(Connection connection) {
            boolean removed = connections.remove(connection);
            if (removed) {
                count.getAndDecrement();
            }
            return removed;
        }

        int size() {
            return count.get();
        }

        Connection register(SocketChannel channel) {
            Connection connection = new Connection(channel, System.currentTimeMillis());
            add(connection);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Server connection from " + connection +
                        "; # active connections: " + size() +
                        "; # queued calls: " + callQueue.size());
            }
            return connection;
        }

        private boolean close(Connection connection) {
            boolean exists = remove(connection);
            if (exists) {
                LOG.debug(Thread.currentThread().getName() +
                        ": disconnecting client " + connection +
                        ". Number of active connections: "+ size());
                connection.close();
            }
            return exists;
        }

        private void closeAll() {
            for (Connection connection : connections) {
                close(connection);
            }
        }

        /**
         * 关闭空闲的连接
         * @param scanAll 是否扫描所有的 connection
         */
        synchronized void closeIdle(boolean scanAll) {
            long minLastContact = System.currentTimeMillis() - maxIdleTime;

            // 下面迭代的过程中有可能遍历不到新插入的 Connection 对象
            // 但是没有关系，因为新的 Connection 不会是空闲的
            int closed = 0;
            for (Connection connection : connections) {
                // 不需要扫全部的情况下，如果没有达到扫描空闲 connection 的阈值
                // 或者下面代码关闭连接导致剩余连接小于 idleScanThreshold 时，便退出循环
                if (!scanAll && size() < idleScanThreshold) {
                    break;
                }

                // 关闭空闲的连接，由于 java && 的短路运算，
                // 如果 scanAll == false，最多关闭 maxIdleToClose 个连接，否则全关闭
                if (connection.isIdle() &&
                        connection.getLastContact() < minLastContact &&
                        close(connection) &&
                        !scanAll && (++closed == maxIdleToClose)) {
                    break;
                }
            }
        }

        void startIdleScan() {
            scheduleIdleScanTask();
        }

        void stopIdleScan() {
            idleScanTimer.cancel();
        }

        private void scheduleIdleScanTask() {
            if (!running) {
                return;
            }
            TimerTask idleCloseTask = new TimerTask() {
                @Override
                public void run() {
                    if(!running) {
                        return;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName()+": task running");
                    }

                    try {
                        closeIdle(false);
                    } finally {
                        // 定时器只调度一次，所以本次任务执行完后手动再次添加到定时器中
                        scheduleIdleScanTask();
                    }
                }
            };
            idleScanTimer.schedule(idleCloseTask, idleScanInterval);
        }
    }
}
