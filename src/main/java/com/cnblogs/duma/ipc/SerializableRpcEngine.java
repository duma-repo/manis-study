package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.ObjectWritable;
import com.cnblogs.duma.io.Writable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 * @author duma
 */
public class SerializableRpcEngine implements RpcEngine {
    public static final Log LOG = LogFactory.getLog(SerializableRpcEngine.class);

    private static class Invocation implements Writable {
        private String methodName;
        private Class<?>[] parameterClasses;
        private Object[] parameters;
        private long clientVersion;
        private String declaringClassProtocolName;

        /**
         * 无参构造，为了在反序列化时使用反射实例化对象
         */
        @SuppressWarnings("unused")
        public Invocation() {}

        public Invocation(Method method, Object[] args, long clientVersion) {
            this.methodName = method.getName();
            this.parameterClasses = method.getParameterTypes();
            this.parameters = args;
            this.declaringClassProtocolName =
                    RPC.getProtocolName(method.getDeclaringClass());
            this.clientVersion = clientVersion;
        }

        public String getMethodName() {
            return methodName;
        }

        public Class<?>[] getParameterClasses() {
            return parameterClasses;
        }

        public Object[] getParameters() {
            return parameters;
        }

        public String getDeclaringClassProtocolName() {
            return declaringClassProtocolName;
        }

        public long getClientVersion() {
            return clientVersion;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            ByteArrayOutputStream byteArrOut = new ByteArrayOutputStream();
            ObjectOutputStream objOut = new ObjectOutputStream(byteArrOut);

            objOut.writeObject(declaringClassProtocolName);
            objOut.writeObject(methodName);
            objOut.writeLong(clientVersion);
            objOut.writeObject(parameters);
            objOut.writeObject(parameterClasses);
            objOut.flush();

            out.writeInt(byteArrOut.toByteArray().length);
            out.write(byteArrOut.toByteArray());
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            int length = in.readInt();
            byte[] byteArr = new byte[length];
            in.readFully(byteArr);

            ByteArrayInputStream byteArrIn = new ByteArrayInputStream(byteArr);
            ObjectInputStream objIn = new ObjectInputStream(byteArrIn);

            try {
                declaringClassProtocolName = (String) objIn.readObject();
                methodName = (String) objIn.readObject();
                clientVersion = objIn.readLong();
                parameters = (Object []) objIn.readObject();
                parameterClasses = (Class<?>[])objIn.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new IOException("Class not found when deserialize.");
            }
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            buffer.append(methodName);
            buffer.append("(");
            for (int i = 0; i < parameters.length; i++) {
                if (i != 0) {
                    buffer.append(", ");
                }
                buffer.append(parameters[i]);
            }
            buffer.append(")");
            return buffer.toString();
        }
    }

    private static class Invoker implements RpcInvocationHandler {
        private Client.ConnectionId remoteId;
        private Client client;
        private final long clientProtocolVersion;

        private Invoker(Class<?> protocol, InetSocketAddress address,
                        Configuration conf, SocketFactory factory,
                        int rpcTimeOut)
                throws IOException {
            this.remoteId = new Client.ConnectionId(address, protocol, rpcTimeOut, conf);
            this.client = new Client(ObjectWritable.class, conf, factory);
            this.clientProtocolVersion = RPC.getProtocolVersion(protocol);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            long startTime = 0;
            if (LOG.isDebugEnabled()) {
                startTime = System.currentTimeMillis();
            }
            ObjectWritable value;
            value = (ObjectWritable) client.call(RPC.RpcKind.RPC_SERIALIZABLE,
                    new Invocation(method, args, clientProtocolVersion), this.remoteId);
            if (LOG.isDebugEnabled()) {
                long callTime = System.currentTimeMillis() - startTime;
                LOG.debug("Call " + method.getName() + " " + callTime);
            }
            return value.get();
        }

        @Override
        public void close() throws IOException {
            client.stop();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getProxy(Class<T> protocol, long clientVersion,
                          InetSocketAddress address, Configuration conf,
                          SocketFactory factory, int rpcTimeOut)
            throws IOException {

        final Invoker invoker = new Invoker(protocol, address, conf, factory, rpcTimeOut);
        return (T) Proxy.newProxyInstance(protocol.getClassLoader(), new Class[]{protocol}, invoker);
    }

    @Override
    public RPC.Server getServer(Class<?> protocol, Object instance,
                                String bindAddress, int port,
                                int numHandlers, int numReaders, int queueSizePerHandler,
                                boolean verbose, Configuration conf) throws IOException {
        return null;
    }

    public static class Server extends RPC.Server {

        public Server(String bindAddress, int port, int numHandlers
                , int numReaders, int queueSizePerHandler
                , Configuration conf) throws IOException {
            super(bindAddress, port, numHandlers, numReaders, queueSizePerHandler, conf);
        }

        static class SerializableRpcInvoker implements RPC.RpcInvoker {
            @Override
            public Writable call(RPC.Server server, String protocol,
                                 Writable rpcRequest, long receiveTime) throws Exception {
                Invocation request = (Invocation) rpcRequest;

                String methodName = request.getMethodName();
                String protoName = request.getDeclaringClassProtocolName();
                long clientVer = request.getClientVersion();
                if (server.verbose) {
                    LOG.info("Call: protocol=" + protocol + ", method=" + methodName);
                }
                ProtoClassProtoImpl protocolImpl = RPC.getProtocolImp(RPC.RpcKind.RPC_SERIALIZABLE
                        , server, protoName, clientVer);

                long startTime = System.currentTimeMillis();
                int qTime = (int) (startTime - receiveTime);
                Exception exception = null;
                try {
                    Method method = protocolImpl.protocolClass
                            .getMethod(methodName, request.getParameterClasses());
                    method.setAccessible(true);
                    Object value =
                            method.invoke(protocolImpl.protocolImpl, request.getParameters());
                    return new ObjectWritable(method.getReturnType(), value);
                } catch (InvocationTargetException e) {
                    Throwable target = e.getTargetException();
                    if (target instanceof IOException) {
                        exception = (IOException) target;
                        throw (IOException) target;
                    } else {
                        IOException ioe = new IOException(target.toString());
                        ioe.setStackTrace(target.getStackTrace());
                        exception = ioe;
                        throw ioe;
                    }
                } catch (Throwable e) {
                    if (! (e instanceof IOException)) {
                        LOG.error("Unexpected throwable object ", e);
                    }
                    IOException ioe = new IOException(e.toString());
                    ioe.setStackTrace(e.getStackTrace());
                    exception = ioe;
                    throw ioe;
                } finally {
                    int processingTime = (int) (System.currentTimeMillis() - startTime);
                    if (LOG.isDebugEnabled()) {
                        String msg = "Served: " + request.getMethodName() +
                                " queueTime= " + qTime +
                                " procesingTime= " + processingTime;
                        if (exception != null) {
                            msg += " exception= " + exception.getClass().getSimpleName();
                        }
                        LOG.debug(msg);
                    }
                }
            }
        }
    }
}
