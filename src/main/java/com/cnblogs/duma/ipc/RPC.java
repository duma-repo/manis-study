package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/**
 * @author duma
 */
public class RPC {
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
                Constructor constructor =  clazz.getDeclaredConstructor();
                engine = (RpcEngine)constructor.newInstance();
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

    public static <T> T getProtocolProxy(Class<T> protocol,
                                         long clientVersion,
                                         InetSocketAddress address,
                                         Configuration conf,
                                         SocketFactory factory,
                                         int rpcTimeOut)
            throws IOException {

        /**
         * todo 解析为什么需要new对象，而不是将getProxy定义成静态方法
         * 静态方法和单例对象的区别
         */
        return getProtocolEngine(protocol, conf).getProxy(protocol, clientVersion,
                address, conf, factory, rpcTimeOut);
    }

}
