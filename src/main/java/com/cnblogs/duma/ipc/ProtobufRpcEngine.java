package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.DataOutputOutputStream;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcRequestHeaderProto;
import com.cnblogs.duma.util.ProtoUtil;
import com.google.protobuf.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 * @author duma
 */
public class ProtobufRpcEngine implements RpcEngine {
    public static  final Log LOG = LogFactory.getLog(ProtobufRpcEngine.class);

    static {
        com.cnblogs.duma.ipc.Server.registerProtocolEngine(
                RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                RpcRequestWrapper.class,
                new Server.ProtobufRpcInvoker());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol,
                          long clientVersion,
                          InetSocketAddress address,
                          Configuration conf,
                          SocketFactory factory,
                          int rpcTimeOut) throws IOException {
        Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeOut);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    }

    private static class Invoker implements RpcInvocationHandler {
        private Client client;
        private Client.ConnectionId remoteId;
        private final String protocolName;
        private final long clientProtocolVersion;
        private final int NORMAL_ARGS_LEN = 2;

        private Invoker(Class<?> protocol,
                        InetSocketAddress address,
                        Configuration conf,
                        SocketFactory factory,
                        int rpcTimeOut) {
            this.protocolName = RPC.getProtocolName(protocol);
            this.clientProtocolVersion = RPC.getProtocolVersion(protocol);
            this.remoteId = new Client.ConnectionId(address, protocol, rpcTimeOut, conf);
            this.client = new Client(RpcResponseWrapper.class, conf, factory);
        }

        private RequestHeaderProto constructRpcRequesHeader(Method method) {
            RequestHeaderProto.Builder headerBuilder = RequestHeaderProto
                    .newBuilder();
            headerBuilder.setMethodName(method.getName());
            headerBuilder.setDeclaringClassProtocolName(protocolName);
            headerBuilder.setClientProtocolVersion(clientProtocolVersion);

            return headerBuilder.build();
        }

        /**
         * RPC 在客户端的 invoker
         * 上层希望仅有 ServiceException 异常被抛出，因此该方法仅抛出 ServiceException 异常
         *
         * 以下两种情况都构造 ServiceException :
         * <ol>
         * <li>该方法中客户端抛出的异常</li>
         * <li>服务端的异常包装在 RemoteException 中的异常</li>
         * </ol>
         *
         * @param proxy proxy
         * @param method 调用的方法
         * @param args 参数
         * @return 返回值
         * @throws ServiceException 异常
         */
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }

            if (args.length != NORMAL_ARGS_LEN) {
                throw new ServiceException("Too many parameters for request. Method: ["
                        + method.getName() + "]" + ", Expected: 2, Actual: "
                        + args.length);
            }
            if (args[1] == null) {
                throw new ServiceException("null param while calling Method: ["
                        + method.getName() + "]");
            }

            RequestHeaderProto header = constructRpcRequesHeader(method);
            Message theRequest = (Message) args[1];
            final RpcResponseWrapper res;
            try {
                res = (RpcResponseWrapper) client.call(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                        new RpcRequestWrapper(header, theRequest), remoteId);
            } catch (Throwable e) {
                throw new ServiceException(e);
            }

            if (LOG.isDebugEnabled()) {
                long callTime = System.currentTimeMillis() - startTime;
                LOG.debug("Call: " + method.getName() + " took " + callTime + "ms");
            }

            Message prototype = null;
            try {
                prototype = getReturnType(method);
            } catch (Exception e) {
                throw new ServiceException(e);
            }
            Message returnMessage = null;
            try {
                returnMessage = prototype.newBuilderForType()
                        .mergeFrom(res.theResponseRead)
                        .build();
            } catch (Throwable t) {
                throw new ServiceException(t);
            }

            return returnMessage;
        }

        private Message getReturnType(Method method) throws Exception {
            Class<?> returnType = method.getReturnType();
            Method newInstMethod = returnType.getMethod("getDefaultInstance");
            newInstMethod.setAccessible(true);
            return (Message) newInstMethod.invoke(null, (Object []) null);
        }

        @Override
        public void close() throws IOException {
            client.stop();
        }
    }

    interface RpcWrapper extends Writable {
        int getLength();
    }

    private static abstract class BaseRpcMessageWithHeader<T extends GeneratedMessage>
            implements RpcWrapper {
        T requestHeader;
        /**
         * 用于客户端
         */
        Message theRequest;
        /**
         * 用于服务端
         */
        byte[] theRequestRead;

        public BaseRpcMessageWithHeader() {}

        public BaseRpcMessageWithHeader(T requestHeader, Message theRequest) {
            this.requestHeader = requestHeader;
            this.theRequest = theRequest;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            OutputStream os = DataOutputOutputStream.constructDataOutputStream(out);

            requestHeader.writeDelimitedTo(os);
            theRequest.writeDelimitedTo(os);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            requestHeader = parseHeaderFrom(readVarIntBytes(in));
            theRequestRead = readMessageRequest(in);
        }

        /**
         * 子类会覆盖该方法
         * @param in
         * @return
         */
        byte[] readMessageRequest(DataInput in) throws IOException {
            return readVarIntBytes(in);
        }

        private byte[] readVarIntBytes(DataInput in) throws IOException {
            int length = ProtoUtil.readRawVarInt32(in);
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return bytes;
        }

        /**
         * 子类覆盖该方法
         * @param bytes 序列化数据（字节数组）
         * @return Header类型
         * @throws IOException
         */
        abstract T parseHeaderFrom(byte[] bytes) throws IOException;

        /**
         * 包括两部分
         * 1. header 序列化后的长度及长度本身的 varInt32 编码后的长度
         * 2. request 序列化后的长度以及长度本身 varInt32 编码后的长度
         * @return 序列化后的数据长度
         */
        @Override
        public int getLength() {
            int headerLen = requestHeader.getSerializedSize();
            int requestLen;
            if (theRequest != null) {
                requestLen = theRequest.getSerializedSize();
            } else if (theRequestRead != null) {
                requestLen = theRequestRead.length;
            } else {
                throw new IllegalArgumentException("getLength on uninitialized RpcWrapper");
            }
            return CodedOutputStream.computeRawVarint32Size(headerLen) + headerLen +
                    CodedOutputStream.computeRawVarint32Size(requestLen) + requestLen;
        }
    }

    private static class RpcRequestWrapper extends
            BaseRpcMessageWithHeader<RequestHeaderProto> {
        @SuppressWarnings("unused")
        public RpcRequestWrapper() {}

        public RpcRequestWrapper(RequestHeaderProto requestHeader, Message theRequest) {
            super(requestHeader, theRequest);
        }

        @Override
        RequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException {
            return RequestHeaderProto.parseFrom(bytes);
        }

        @Override
        public String toString() {
            return requestHeader.getDeclaringClassProtocolName() + "." +
                    requestHeader.getMethodName();
        }
    }

    public static class RpcRequestMessageWrapper extends
            BaseRpcMessageWithHeader<RpcRequestHeaderProto> {

        public RpcRequestMessageWrapper(RpcRequestHeaderProto requestHeader, Message theRequest) {
            super(requestHeader, theRequest);
        }

        @Override
        RpcRequestHeaderProto parseHeaderFrom(byte[] bytes) throws IOException {
            return RpcRequestHeaderProto.parseFrom(bytes);
        }
    }

    /**
     * Protocol Buffer 响应信息的 wrapper
     *
     */
    public static class RpcResponseWrapper implements RpcWrapper {
        Message theRespone;
        byte[] theResponseRead;

        public RpcResponseWrapper() {
        }

        public RpcResponseWrapper(Message theRespone) {
            this.theRespone = theRespone;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            OutputStream os = DataOutputOutputStream.constructDataOutputStream(out);

            theRespone.writeDelimitedTo(os);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int len = ProtoUtil.readRawVarInt32(in);
            theResponseRead = new byte[len];
            in.readFully(theResponseRead);
        }

        @Override
        public int getLength() {
            int resLen = 0;
            if (theRespone != null) {
                resLen = theRespone.getSerializedSize();
            } else if (theResponseRead != null) {
                resLen = theResponseRead.length;
            } else {
                throw new IllegalArgumentException(
                        "getLength on uninitialized RpcWrapper");
            }
            return CodedOutputStream.computeRawVarint32Size(resLen) + resLen;
        }
    }

    @Override
    public RPC.Server getServer(Class<?> protocol, Object instance,
                                String bindAddress, int port,
                                int numHandlers, int numReaders,
                                int queueSizePerHandler, boolean verbose,
                                Configuration conf) throws IOException {
        return new Server(protocol, instance, bindAddress, port,
                numHandlers, numReaders, queueSizePerHandler, verbose, conf);
    }

    private static class Server extends RPC.Server {
        /**
         * 构造 protocol buffer rpc server
         * @param protocol 接口（协议）
         * @param protocolImpl 接口（协议）的实例
         * @param bindAddress 服务端地址
         * @param port 服务端端口
         * @param numHandlers handler 线程个数
         * @param numReaders reader 线程个数
         * @param queueSizePerHandler 每个 Handler 期望的消息队列大小
         * @param verbose 是否对调用信息打log
         * @param conf Configuration 对象
         */
        public Server(Class<?> protocol, Object protocolImpl,
                      String bindAddress, int port,
                      int numHandlers, int numReaders,
                      int queueSizePerHandler, boolean verbose,
                      Configuration conf) throws IOException {
            super(bindAddress, port, numHandlers, numReaders, queueSizePerHandler, conf);
            this.verbose = verbose;
            registerProtocolAndImpl(RPC.RpcKind.RPC_PROTOCOL_BUFFER, protocol, protocolImpl);
        }

        static class ProtobufRpcInvoker implements RPC.RpcInvoker {
            @Override
            public Writable call(RPC.Server server, String protocol,
                                 Writable rpcRequest, long receiveTime)
                    throws Exception {
                RpcRequestWrapper request = (RpcRequestWrapper) rpcRequest;
                RequestHeaderProto requestHeader = request.requestHeader;
                String methodName = requestHeader.getMethodName();
                String protoName = requestHeader.getDeclaringClassProtocolName();
                long clientVer = requestHeader.getClientProtocolVersion();
                if (server.verbose) {
                    LOG.info("Call: protocol=" + protocol + ", method=" + methodName);
                }
                ProtoClassProtoImpl protoClassImpl = RPC.getProtocolImp(RPC.RpcKind.RPC_PROTOCOL_BUFFER,
                        server, protoName, clientVer);
                BlockingService service = (BlockingService) protoClassImpl.protocolImpl;
                Descriptors.MethodDescriptor methodDescriptor =
                        service.getDescriptorForType().findMethodByName(methodName);
                if (methodDescriptor == null) {
                    String msg = "Unknown method " + methodName
                            + " called on " + protocol + " protocol.";
                    LOG.warn(msg);
                    throw new RpcNoSuchMethodException(msg);
                }
                Message protoType = service.getRequestPrototype(methodDescriptor);
                Message param = protoType.newBuilderForType()
                        .mergeFrom(request.theRequestRead)
                        .build();
                Message result;
                long startTime = System.currentTimeMillis();
                // 从接受请求到调用前的时间
                int qTime = (int) (startTime - receiveTime);
                Exception exception = null;
                try {
                    result = service.callBlockingMethod(methodDescriptor, null, param);
                } catch (ServiceException se) {
                    // callBlockingMethod 有可能抛出该异常
                    exception = (Exception) se.getCause();
                    throw exception;
                } catch (Exception e) {
                    exception = e;
                    throw exception;
                } finally {
                    long processTime = (int) (System.currentTimeMillis() - startTime);
                    if (LOG.isDebugEnabled()) {
                        String msg = "Served: " + methodName + " queueTime= " + qTime +
                                " processingTime= " + processTime;
                        if (exception != null) {
                            msg += " exception= " + exception.getClass().getSimpleName();
                        }
                        LOG.debug(msg);
                    }
                }
                return new RpcResponseWrapper(result);
            }
        }
    }
}
