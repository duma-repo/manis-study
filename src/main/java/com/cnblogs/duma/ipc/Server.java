package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.protobuf.IpcConnectionContextProtos.*;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.*;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;
import com.cnblogs.duma.util.ProtoUtil;
import com.cnblogs.duma.util.ReflectionUtils;
import com.cnblogs.duma.util.StringUtils;
import com.google.protobuf.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.lang.reflect.Constructor;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
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

    /**
     * callQueue 队列的大小
     */
    private int maxQueueSize;
    /**
     * 响应 buffer 的上限
     */
    private int maxRespSize;
    /**
     * 客户端信息长度的上限
     */
    private int maxDataLength;
    private boolean tcpNoDelay;
    /**
     * pendingConnections 队列的大小
     */
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
     *
     * @param rpcKind                RPC.RpcKind 对象
     * @param rpcRequestWrapperClass 调用请求的封装类的Class对象
     * @param rpcInvoker             服务端方法调用类对象
     */
    static void registerProtocolEngine(RPC.RpcKind rpcKind,
                                       Class<? extends Writable> rpcRequestWrapperClass,
                                       RPC.RpcInvoker rpcInvoker) {
        RpcKindMapValue rpcKindMapValue =
                new RpcKindMapValue(rpcRequestWrapperClass, rpcInvoker);
        RpcKindMapValue old = rpcKindMap.put(rpcKind, rpcKindMapValue);
        if (old != null) {
            rpcKindMap.put(rpcKind, old);
            throw new IllegalArgumentException("ReRegistration of rpcKind: " + rpcKind);
        }
        LOG.debug("rpcKind=" + rpcKind +
                ", rpcRequestWrapperClass=" + rpcRequestWrapperClass +
                ", rpcInvoker=" + rpcInvoker);
    }

    Class<? extends Writable> getRpcRequestWrapper(
            RpcHeaderProtos.RpcKindProto rpcKind) {
        RpcKindMapValue val = rpcKindMap.get(ProtoUtil.convertRpcKind(rpcKind));
        return (val == null) ? null : val.rpcRequestWrapperClass;
    }

    public static RPC.RpcInvoker getRpcInvoker(RPC.RpcKind rpcKind) {
        RpcKindMapValue val = rpcKindMap.get(rpcKind);
        return (val == null) ? null : val.rpcInvoker;
    }

    /**
     * Server 类构造方法
     *
     * @param bindAddress         服务端地址
     * @param port                服务端端口
     * @param numHandlers         Handler 线程的数量
     * @param numReaders          Reader 线程的数量
     * @param queueSizePerHandler 每个 Handler 期望的消息队列大小，再根据 numHandlers 可以得出队列总大小
     * @param conf                配置
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
     * 构造服务端响应信息，并赋值给 Call 对象
     *
     * @param responseBuf 序列化响应信息的 buffer
     * @param call        {@link Call} 对象
     * @param status      调用状态
     * @param errorCode   调用错误码
     * @param resValue    如果调用成功，代表调用的返回值。如果调用失败则为 null
     * @param errorClass  error class
     * @param error       调用错误的堆栈信息
     * @throws IOException
     */
    private void setupResponse(ByteArrayOutputStream responseBuf,
                               Call call, RpcStatusProto status, RpcErrorCodeProto errorCode,
                               Writable resValue, String errorClass, String error) {

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

    public abstract Writable call(RPC.RpcKind rpcKind, String protocol,
                                  Writable param, long receiveTime) throws Exception;

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

    /**
     * 当读写 buffer 的大小超过 8KB 限制，读写操作的数据将按照该值分割。
     * 大部分 RPC 不会拆过这个值
     */
    private static int NIO_BUFFER_LIMIT = 8*1024;

    /**
     * 该函数是 {@link ReadableByteChannel#read(ByteBuffer)} 的 wrapper
     * 如果需要读数据量较大, 可以分成小块从 channel 读数据。这样，可以避免
     * 随着 ByteBuffer 大小的增加，jdk 创建大量 buffer，从而避免性能衰减
     *
     * @see ReadableByteChannel#read(ByteBuffer)
     */
    private int channelRead(ReadableByteChannel channel,
                            ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.read(buffer) : channelIO(channel, null, buffer);
        return count;
    }

    /**
     * 该函数是 {@link WritableByteChannel#write(ByteBuffer)} 的 wrapper
     * 如果需要读数据量较大, 可以分成小块从 channel 写数据。这样，可以避免
     * 随着 ByteBuffer 大小的增加，jdk 创建大量 buffer，从而避免性能衰减
     *
     * @see WritableByteChannel#write(ByteBuffer)
     * @param channel
     * @param buffer
     * @return 写入的字节数
     * @throws IOException
     */
    private int channelWrite(WritableByteChannel channel,
                             ByteBuffer buffer) throws IOException {
        int count = (buffer.remaining() <= NIO_BUFFER_LIMIT) ?
                channel.write(buffer) : channelIO(null, channel, buffer);
        return count;
    }

    /**
     * 通道的分批读写，分批的单位是 NIO_BUFFER_LIMIT
     * readCh 和 writeCh 仅有一个非空，当 readCh 非空代表读通道，当 writeCh 非空代表写通道
     */
    private static int channelIO(ReadableByteChannel readCh,
                                 WritableByteChannel writeCh,
                                 ByteBuffer buf) throws IOException {

        int originalLimit = buf.limit();
        int initialRemaining = buf.remaining();
        int ret = 0;

        while (buf.remaining() > 0) {
            try {
                int ioSize = Math.min(buf.remaining(), NIO_BUFFER_LIMIT);
                buf.limit(buf.position() + ioSize);

                ret = (readCh == null) ? writeCh.write(buf) : readCh.read(buf);

                if (ret < ioSize) {
                    // 说明当前批次的数据没有度写完，无需再循环
                    break;
                }
            } finally {
                // 将 limit 回复初始值，以进行下一次读写
                buf.limit(originalLimit);
            }
        }

        int nBytes = initialRemaining - buf.remaining();
        return (nBytes > 0) ? nBytes : ret;
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

            @Override
            public void run() {
                LOG.info("Starting " + Thread.currentThread().getName());
                try {
                    doRunLoop();
                } finally {
                    try {
                        readSelector.close();
                    } catch (IOException ioe) {
                        LOG.error("Error closing read selector in " + Thread.currentThread().getName(), ioe);
                    }
                }
            }

            void doRunLoop() {
                while (running) {
                    SelectionKey key = null;
                    try {
                        // 只消费当前已入队列的 Connection 对象
                        // 应该避免在队列上阻塞等待，导致后面 select 方法得不到调用
                        int size = pendingConnections.size();
                        for (int i = 0; i < size; i++) {
                            Connection conn = pendingConnections.take();
                            conn.channel.register(readSelector, SelectionKey.OP_READ, conn);
                        }
                        readSelector.select();

                        Iterator<SelectionKey> iter = readSelector.selectedKeys().iterator();
                        while (iter.hasNext()) {
                            key = iter.next();
                            iter.remove();
                            if (key.isValid() &&
                                    key.isReadable()) {
                                doRead(key);
                            }
                            key = null;
                        }
                    } catch (InterruptedException e) {
                        if (running) {
                            LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                        }
                    } catch (IOException e) {
                        LOG.error("Error in Reader", e);
                    }
                }
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

        void doRead(SelectionKey key) throws InterruptedException {
            int count = 0;
            Connection conn = (Connection)key.attachment();
            if(conn == null) {
                return;
            }
            conn.setLastContact(System.currentTimeMillis());

            try {
                count = conn.readAndProcess();
            } catch (InterruptedException ie) {
                LOG.info(Thread.currentThread().getName() +
                        " readAndProcess caught InterruptedException", ie);
                throw ie;
            } catch (Exception e) {
                // 这层进行异常的捕获，因为 WrappedRpcServerException 异常已经发送响应信息给客户端了，无需记录栈踪
                // 但其他异常属于服务器内部异常，需要记录
                LOG.info(Thread.currentThread().getName() + ": readAndProcess from client " +
                                conn.getHostAddress() + " throw exception [" + e + " ]",
                        (e instanceof WrappedRpcServerException) ? null : e);
                // 为了关闭连接， 将 count 置为 -1
                count = -1;
            }
            if (count < 0) {
                closeConnection(conn);
                conn = null;
            } else {
                conn.setLastContact(System.currentTimeMillis());
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

        @Override
        public void run() {
            LOG.debug(Thread.currentThread().getName() + ": starting.");
            ByteArrayOutputStream buf = new ByteArrayOutputStream(10240);
            while (running) {
                try {
                    final Call call = callQueue.take();
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Thread.currentThread().getName() + ": " + call +
                                " for rpcKind " + call.rpcKind);
                    }
                    if (!call.connection.channel.isOpen()) {
                        LOG.info(Thread.currentThread().getName() + ": skipped " + call);
                        continue;
                    }
                    String errorClass = null;
                    String error = null;
                    RpcStatusProto returnStatus = RpcStatusProto.SUCCESS;
                    RpcErrorCodeProto detailedErr = null;
                    Writable value = null;

                    try {
                        value = call(call.rpcKind, call.connection.protocolName,
                                call.rpcRequest, call.timestamp);
                    } catch (Throwable e) {
                        String logMsg = Thread.currentThread().getName() + ", call " + call;
                        if (e instanceof RuntimeException || e instanceof Error) {
                            // 抛出该类型的错误说明服务端自身出现问题
                            LOG.warn(logMsg, e);
                        } else {
                            // 属于正常情况的异常抛出
                            LOG.info(logMsg, e);
                        }
                        if (e instanceof RpcServerException) {
                            RpcServerException rse = (RpcServerException) e;
                            returnStatus = rse.getRpcStatusProto();
                            detailedErr = rse.getRpcErrorCodeProto();
                        } else {
                            returnStatus = RpcStatusProto.ERROR;
                            detailedErr = RpcErrorCodeProto.ERROR_APPLICATION;
                        }
                        errorClass = e.getClass().getName();
                        error = StringUtils.stringifyException(e);
                    }
                    setupResponse(buf, call, returnStatus,
                            detailedErr, value, errorClass, error);

                    //如果 buf 占用空间太大则丢弃掉，重新将 buf 调到初始大小以释放堆内存
                    if (buf.size() > maxRespSize) {
                        LOG.info("Large response size " + buf.size() + " for call "
                                + call.toString());
                        buf = new ByteArrayOutputStream(10240);
                    }
                    responder.doResponse(call);
                } catch (InterruptedException e) {
                    LOG.info(Thread.currentThread().getName() + " unexpectedly interrupted", e);
                } catch (Exception e) {
                    LOG.info(Thread.currentThread().getName() + " caught an exception", e);
                }
            }
            LOG.debug(Thread.currentThread().getName() + ": exiting");
        }
    }

    /**
     * 向客户端发送响应结果
     */
    private class Responder extends Thread {
        void doResponse(Call call) throws IOException {

        }
    }

    private static class WrappedRpcServerException extends RpcServerException {
        private RpcErrorCodeProto errorCode;

        public WrappedRpcServerException(RpcErrorCodeProto errCode, IOException ioe) {
            super(ioe.toString(), ioe);
            this.errorCode = errCode;
        }

        public WrappedRpcServerException(RpcErrorCodeProto errorCode, String message) {
            this(errorCode, new RpcServerException(message));
        }

        @Override
        public RpcErrorCodeProto getRpcErrorCodeProto() {
            return errorCode;
        }

        @Override
        public String toString() {
            return getCause().toString();
        }
    }

    private class Connection {
        private SocketChannel channel;
        private volatile long lastContact;

        private Socket socket;
        private InetAddress remoteAddr;
        private String hostAddress;
        private int remotePort;

        private volatile int rpcCount;

        private ByteBuffer dataLengthBuffer;
        private ByteBuffer connectionHeaderBuf = null;
        private ByteBuffer data;

        /** 连接头是否被读过 */
        private boolean connectionHeaderRead = false;
        /** 连接头后的连接上下文是否被读过 */
        private boolean connectionContextRead = false;

        IpcConnectionContextProto connectionContext;
        String protocolName;

        public String getHostAddress() {
            return hostAddress;
        }

        Connection(SocketChannel channel, long lastContact) {
            this.channel = channel;
            this.lastContact = lastContact;
            this.socket = channel.socket();
            this.remoteAddr = socket.getInetAddress();
            this.hostAddress = remoteAddr.getHostAddress();
            this.remotePort = socket.getPort();

            this.dataLengthBuffer = ByteBuffer.allocate(4);
        }

        @Override
        public String toString() {
            return getHostAddress() + ":" + remotePort;
        }

        private void checkDataLength(int dataLength) throws IOException {
            if (dataLength < 0) {
                String errMsg = "Unexpected data length " + dataLength +
                        "! from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
            if (dataLength > maxDataLength) {
                String errMsg = "Requested data length " + dataLength +
                        " is longer than maximum configured RPC length " +
                        maxDataLength + ".  RPC came from " + hostAddress;
                LOG.warn(errMsg);
                throw new IOException(errMsg);
            }
        }

        int readAndProcess() throws IOException, InterruptedException {
            while (true) {
                // 至少读一次 RPC 的数据
                // 一直迭代直到一次 RPC 的数据读完或者没有剩余的数据
                int count = -1;
                if (dataLengthBuffer.remaining() > 0) {
                    count = channelRead(channel, dataLengthBuffer);
                    if (count < 0 || dataLengthBuffer.remaining() > 0) {
                        // 读取有问题或者未读完，直接返回
                        return count;
                    }
                }
                if (!connectionHeaderRead) {
                    if (connectionHeaderBuf == null) {
                        connectionHeaderBuf = ByteBuffer.allocate(3);
                    }
                    count = channelRead(channel, connectionHeaderBuf);
                    if (count < 0 || connectionHeaderBuf.remaining() > 0) {
                        return count;
                    }
                    int version = connectionHeaderBuf.get(0);
                    int serviceClass = connectionHeaderBuf.get(1);

                    dataLengthBuffer.flip();
                    if (!RpcConstants.HEADER.equals(dataLengthBuffer) ||
                            version != RpcConstants.CURRENT_VERSION) {
                        LOG.warn("Incorrect header or version mismatch from " +
                                hostAddress + ":" + remotePort +
                                " got version " + version +
                                " expected version " + RpcConstants.CURRENT_VERSION);
                        return -1;
                    }

                    dataLengthBuffer.clear();
                    connectionHeaderBuf = null;
                    connectionHeaderRead = true;
                    continue;
                }

                if (data == null) {
                    dataLengthBuffer.flip();
                    int dataLength = dataLengthBuffer.getInt();
                    checkDataLength(dataLength);
                    data = ByteBuffer.allocate(dataLength);
                }

                count = channelRead(channel, data);
                if (data.remaining() == 0) {
                    // 说明数据已经完全读入
                    dataLengthBuffer.clear();
                    data.flip();
                    // processOneRpc 函数可能改变 connectionContextRead 值
                    // 需要将当前值存储在临时变量中
                    boolean isHeaderRead = connectionContextRead;
                    processOneRpc(data.array());
                    data = null;
                    if (!isHeaderRead) {
                        continue;
                    }
                }
                return count;
            }
        }

        /**
         * 处理一次 RPC 请求
         * @param buf RPC 请求的上下文或者调用请求
         */
        private void processOneRpc(byte[] buf)
                throws IOException, InterruptedException {
            int callId = -1;
            int retry = RpcConstants.INVALID_RETRY_COUNT;

            try {
                DataInputStream dis =
                        new DataInputStream(new ByteArrayInputStream(buf));
                RpcRequestHeaderProto header =
                        decodeProtobufFromStream(RpcRequestHeaderProto.newBuilder(), dis);
                callId = header.getCallId();
                retry = header.getRetryCount();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("get #" + callId);
                }
                checkRpcHeader(header);
                if (callId < 0) {
                    processOutOfBandRequest(header, dis);
                } else if (!connectionContextRead) {
                    throw new WrappedRpcServerException(
                            RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                            "Connection context not found");
                } else {
                    processRpcRequest(header, dis);
                }
            } catch (WrappedRpcServerException wrse) {
                Throwable ioe = wrse.getCause();
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                Call call = new Call(callId, retry, null, this);
                setupResponse(buffer, call, RpcStatusProto.FATAL, wrse.getRpcErrorCodeProto(),
                        null, ioe.getClass().getName(), ioe.getMessage());
                responder.doResponse(call);
                throw wrse;
            }
        }

        /**
         * 验证 rpc header 是否正确
         * @param header RPC request header
         * @throws WrappedRpcServerException header 包含无效值
         */
        private void checkRpcHeader(RpcRequestHeaderProto header)
                throws WrappedRpcServerException{
            if (!header.hasRpcKind()) {
                String errMsg = "IPC Server: No rpc kind in rpcRequestHeader";
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER, errMsg);
            }
        }

        private void processRpcRequest(RpcRequestHeaderProto header,
                                       DataInputStream dis)
                throws WrappedRpcServerException, InterruptedException {
            Class<? extends Writable> rpcRequestClass =
                    getRpcRequestWrapper(header.getRpcKind());
            if (rpcRequestClass == null) {
                LOG.warn("Unknown RPC kind " + header.getRpcKind() +
                        " from client " + hostAddress);
                final String err = "Unknown rpc kind in rpc header " + header.getRpcKind();
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        err);
            }
            Writable rpcRequest;
            try {
                rpcRequest = ReflectionUtils.newInstance(rpcRequestClass);
                rpcRequest.readFields(dis);
            } catch (Exception e) {
                LOG.warn("Unable to read call parameters for client " +
                        hostAddress + "on connection protocol " +
                        this.protocolName + " for rpcKind " + header.getRpcKind(),  e);
                String err = "IPC server unable to read call parameters: "+ e.getMessage();
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST, err);
            }
            Call call = new Call(header.getCallId(), header.getRetryCount(), rpcRequest, this,
                    ProtoUtil.convertRpcKind(header.getRpcKind()), header.getClientId().toByteArray());
            // 将 call 入队列，有可能在这里阻塞
            callQueue.put(call);
            incRpcCount();
        }

        /**
         * 处理 out of band 数据
         * @param header RPC header
         * @param dis 请求的数据流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processOutOfBandRequest(RpcRequestHeaderProto header,
                                             DataInputStream dis)
                throws WrappedRpcServerException {
            int callId = header.getCallId();
            if (callId == RpcConstants.CONNECTION_CONTEXT_CALL_ID) {
                processConnectionContext(dis);
            } else {
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Unknown out of band call #" + callId);
            }
        }

        /**
         * 读取 connection context 信息
         * @param dis 客户端请求的数据流
         * @throws WrappedRpcServerException 状态错误
         */
        private void processConnectionContext(DataInputStream dis)
                throws WrappedRpcServerException {
            if (connectionContextRead) {
                throw new WrappedRpcServerException(
                        RpcErrorCodeProto.FATAL_INVALID_RPC_HEADER,
                        "Connection context already processed");
            }
            connectionContext = decodeProtobufFromStream(IpcConnectionContextProto.newBuilder(), dis);
            protocolName = connectionContext.getProtocol();
            connectionContextRead = true;
        }

        private void setLastContact(long lastContact) {
            this.lastContact = lastContact;
        }

        private long getLastContact() {
            return lastContact;
        }

        private synchronized void close() {

        }

        @SuppressWarnings("unchecked")
        private <T extends Message> T decodeProtobufFromStream(Message.Builder builder,
                                                               DataInputStream dis) throws WrappedRpcServerException {
            try {
                builder.mergeDelimitedFrom(dis);
                return (T) builder.build();
            } catch (Exception e) {
                Class<?> protoClass = builder.getDefaultInstanceForType().getClass();
                throw new WrappedRpcServerException(RpcErrorCodeProto.FATAL_DESERIALIZING_REQUEST,
                        "Error decoding " + protoClass.getSimpleName() + ": " + e);
            }
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
