package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.io.Writable;
import com.cnblogs.duma.util.ReflectionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.net.SocketFactory;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author duma
 */
public class RPC {
    static final Log LOG = LogFactory.getLog(RPC.class);
    final static int RPC_SERVICE_CLASS_DEFAULT = 0;
    public enum RpcKind {
        /**
         * RPC_SERIALIZABLE: SerializableRpcEngine
         * RPC_PROTOCOL_BUFFER: ProtoBufRpcEngine
         */
        RPC_BUILTIN ((short) 1),
        RPC_SERIALIZABLE ((short) 2),
        RPC_PROTOCOL_BUFFER ((short) 3);

        final static int MAX_INDEX = RPC_PROTOCOL_BUFFER.value;
        public final short value;


        RpcKind(short value) {
            this.value = value;
        }
    }

    interface RpcInvoker {
        /**
         * 服务端实现该方法，用来完成客户端请求的方法调用
         * @param server RPC.Server对象，用来获取服务端的属性
         * @param protocol 客户端请求的接口（协议）
         * @param rpcRequest 客户端调用请求的封装类对象（反序列化后的）
         * @param receiveTime 开始处理本次 RPC 请求的时间
         * @return 方法调用的返回值
         * @throws Exception
         */
        Writable call(Server server, String protocol,
                      Writable rpcRequest, long receiveTime) throws Exception;
    }

    /**
     * 接口与RPC引擎对应关系的缓存
     */
    private static final Map<Class<?>, RpcEngine> PROTOCOL_ENGINS
            = new HashMap<Class<?>, RpcEngine>();

    public static final String RPC_ENGINE = "rpc.engine";

    /**
     * 为协议（接口）设置RPC引擎
     * @param conf 配置
     * @param protocol 协议接口
     * @param engine 实现的引擎
     */
    public static void setProtocolEngine(Configuration conf,
                                         Class<?> protocol, Class<?> engine) {
        conf.setClass(RPC_ENGINE + "." + protocol.getName(), engine, RpcEngine.class);
    }

    /**
     * 根据协议和配置返回该协议对应的RPC引擎
     * @param protocol
     * @param conf
     * @return
     */
    static synchronized <T> RpcEngine getProtocolEngine(Class<T> protocol, Configuration conf) {
        RpcEngine engine = PROTOCOL_ENGINS.get(protocol);
        if (engine == null) {
            Class<?> clazz = conf.getClass(RPC_ENGINE + "." + protocol.getName(), SerializableRpcEngine.class);

            try {
                // 通过反射实例化RpcEngine的实现类
                engine = (RpcEngine)ReflectionUtils.newInstance(clazz);
                PROTOCOL_ENGINS.put(protocol, engine);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return engine;
    }

    /**
     * 获得协议版本
     * @param protocol
     * @return
     */
    public static long getProtocolVersion(Class<?> protocol) {
        if (protocol == null) {
            throw new IllegalArgumentException("Null protocol");
        }

        long version;
        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        if (anno != null) {
            version = anno.protocolVersion();
            if (version != -1) {
                return version;
            }
        }
        try {
            Field versionField = protocol.getField("versionID");
            versionField.setAccessible(true);
            return versionField.getLong(protocol);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getProtocolName(Class<?> protocol) {
        if (protocol == null) {
            return null;
        }

        ProtocolInfo anno = protocol.getAnnotation(ProtocolInfo.class);
        return anno == null ? protocol.getName() : anno.protocolName();
    }

    public static <T> T getProtocolProxy(Class<T> protocol,
                                         long clientVersion,
                                         InetSocketAddress address,
                                         Configuration conf,
                                         SocketFactory factory,
                                         int rpcTimeOut)
            throws IOException {

        /**
         * 静态方法和单例对象的区别
         */
        return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
                address, conf, factory, rpcTimeOut);
    }

    /**
     * 停止代理，该代理需要实现 {@link Closeable} 或者关联 {@link RpcInvocationHandler}
     * @param proxy 需要停止的代理
     *
     * @throws IllegalArgumentException
     *      代理没有实现 {@link Closeable} 接口
     */
    public static void stopProxy(Object proxy) {
        if (proxy == null) {
            throw new IllegalArgumentException("Cannot close proxy since it is null");
        }

        try {
            if (proxy instanceof Closeable) {
                ((Closeable) proxy).close();
                return;
            }
            InvocationHandler handler = Proxy.getInvocationHandler(proxy);
            if (handler instanceof Closeable) {
                ((Closeable) handler).close();
                return;
            }
        } catch (IOException e) {
            LOG.error("Closing proxy or invocation handler caused exception", e);
        } catch (IllegalArgumentException e) {
            LOG.error("RPC.stopProxy called on non proxy: class=" + proxy.getClass().getName(), e);
        }

        // proxy 没有 close 方法
        throw new IllegalArgumentException(
                "Cannot close proxy - is not Closeable or "
                        + "does not provide closeable invocation handler "
                        + proxy.getClass());
    }

    /**
     * 该类用于构造 RPC Server
     */
    public static class Builder {
        private Class<?> protocol;
        private Object instance;
        private String bindAdress = "0.0.0.0";
        private int bindPort = 0;
        private int numHandlers = 1;
        private int numReaders = -1;
        private boolean verbose = false;
        private int queueSizePerHandler = -1;
        private Configuration conf;

        public Builder(Configuration conf) {
            this.conf = conf;
        }

        public Builder setProtocol(Class<?> protocol) {
            this.protocol = protocol;
            return this;
        }

        public Builder setInstance(Object instance) {
            this.instance = instance;
            return this;
        }

        public Builder setBindAdress(String bindAdress) {
            this.bindAdress = bindAdress;
            return this;
        }

        public Builder setBindPort(int bindPort) {
            this.bindPort = bindPort;
            return this;
        }

        public Builder setNumHandlers(int numHandlers) {
            this.numHandlers = numHandlers;
            return this;
        }

        public Builder setVerbose(boolean verbose) {
            this.verbose = verbose;
            return this;
        }

        public void setConf(Configuration conf) {
            this.conf = conf;
        }

        /**
         * 创建 RPC.Server 实例
         * @return RPC.Server
         * @throws IOException 发生错误
         * @throws IllegalArgumentException 没有设置必要的参数时
         */
        public Server build() throws IOException, IllegalArgumentException {
            if (this.conf == null) {
                throw new IllegalArgumentException("conf is not set");
            }
            if (this.protocol == null) {
                throw new IllegalArgumentException("protocol is not set");
            }
            if (this.instance == null) {
                throw new IllegalArgumentException("instance is not set");
            }

            return getProtocolEngine(this.protocol, this.conf).getServer(
                    this.protocol, this.instance, this.bindAdress, this.bindPort,
                    this.numHandlers, this.numReaders, this.queueSizePerHandler,
                    this.verbose, this.conf);
        }
    }

    static Server.ProtoClassProtoImpl getProtocolImp(
            RPC.RpcKind rpcKind
            , RPC.Server server,
            String protocolName,
            long clientVersion) throws RpcServerException {
        Server.ProtoNameVer pv = new Server.ProtoNameVer(protocolName, clientVersion);
        Server.ProtoClassProtoImpl impl =
                server.getProtocolImplMap(rpcKind).get(pv);
        if (impl == null) {
            throw new RpcNoSuchProtocolException(
                    "Unknown protocol: " + protocolName);
        }
        return impl;
    }

    /**
     * RPC Server
     */
    public abstract static class Server extends com.cnblogs.duma.ipc.Server {
        boolean verbose;

        protected Server(String bindAddress, int port,
                         int numHandlers, int numReaders, int queueSizePerHandler,
                         Configuration conf) throws IOException {
            super(bindAddress, port, numHandlers, numReaders, queueSizePerHandler, conf);
        }

        /**
         * 存储协议（接口）的名称和版本，用来当做 map 的 key
         */
        static class ProtoNameVer {
            final String protoName;
            final long version;

            ProtoNameVer(String protoName, long version) {
                this.protoName = protoName;
                this.version = version;
            }

            @Override
            public boolean equals(Object obj) {
                if (obj == null) {
                    return false;
                }
                if (this == obj) {
                    return true;
                }
                if (! (obj instanceof ProtoNameVer)) {
                    return false;
                }
                ProtoNameVer pv = (ProtoNameVer) obj;
                return (this.protoName.equals(pv.protoName)) &&
                        (this.version == pv.version);
            }

            @Override
            public int hashCode() {
                return protoName.hashCode() * 37 + (int) version;
            }
        }

        /**
         * 存储协议（接口）的 class 及其实现实例
         */
        static class ProtoClassProtoImpl {
            Class<?> protocolClass;
            Object protocolImpl;

            ProtoClassProtoImpl(Class<?> protocolClass, Object protocolImpl) {
                this.protocolClass = protocolClass;
                this.protocolImpl = protocolImpl;
            }
        }

        /** 存储 server 不同序列化方式对应的协议 map */
        ArrayList<Map<ProtoNameVer, ProtoClassProtoImpl>> protocolImplMapArray =
                new ArrayList<>(RpcKind.MAX_INDEX);

        Map<ProtoNameVer, ProtoClassProtoImpl> getProtocolImplMap(RpcKind rpcKind) {
            if (protocolImplMapArray.size() == 0) {
                for (int i = 0; i < RpcKind.MAX_INDEX; i++) {
                    protocolImplMapArray.add(new HashMap<ProtoNameVer, ProtoClassProtoImpl>(10));
                }
            }

            return protocolImplMapArray.get(rpcKind.ordinal());
        }

        /**
         * 注册接口及其实例的键值对
         * @param rpcKind rpc类型
         * @param protocolClass 接口（协议）
         * @param protocolImpl 接口（协议）的实例对象
         */
        void registerProtocolAndImpl(RpcKind rpcKind, Class<?> protocolClass, Object protocolImpl) {
            String protocolName = RPC.getProtocolName(protocolClass);

            long version;
            try {
                version = RPC.getProtocolVersion(protocolClass);
            } catch (Exception e) {
                LOG.warn("Protocol "  + protocolClass +
                        " NOT registered as cannot get protocol version ");
                return;
            }
            getProtocolImplMap(rpcKind).put(new ProtoNameVer(protocolName, version),
                    new ProtoClassProtoImpl(protocolClass, protocolImpl));
            LOG.debug("RpcKind = " + rpcKind + " Protocol Name = " + protocolName +  " version=" + version +
                    " ProtocolImpl=" + protocolImpl.getClass().getName() +
                    " protocolClass=" + protocolClass.getName());
        }

        /**
         * 向 server 中增加接口（协议）及其实例的键值对
         * @param rpcKind
         * @param protocolClass
         * @param protocolImpl
         */
        public void addProtocol(RpcKind rpcKind, Class<?> protocolClass, Object protocolImpl) {
            registerProtocolAndImpl(rpcKind, protocolClass, protocolImpl);
        }

        @Override
        public Writable call(RpcKind rpcKind, String protocol,
                             Writable param, long receiveTime) throws Exception {
            return getRpcInvoker(rpcKind).call(this, protocol, param, receiveTime);
        }
    }
}
