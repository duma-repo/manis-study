package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.ProtobufRpcEngine;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.ipc.SerializableRpcEngine;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.protocol.ManagerManisDbProtocolSerializable;
import com.cnblogs.duma.protocol.ManagerProtocol;
import com.cnblogs.duma.protocolPB.ClientManisDbProtocolPB;
import com.cnblogs.duma.protocolPB.ClientManisDbProtocolTranslatorPB;
import com.cnblogs.duma.server.manisdb.ManisDb;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;


/**
 * @author duma
 */
public class ManisDbProxies {

    public static class ProxyInfo<PROXYTYPE> {
        private final PROXYTYPE proxy;
        private final InetSocketAddress address;

        public ProxyInfo(PROXYTYPE proxy, InetSocketAddress address) {
            this.proxy = proxy;
            this.address = address;
        }

        public PROXYTYPE getProxy() {
            return proxy;
        }

        public InetSocketAddress getAddress() {
            return address;
        }
    }

    private static ManagerProtocol createManisDbProxyWithManagerProtocol(Configuration conf,
            InetSocketAddress address) throws IOException {
        RPC.setProtocolEngine(conf, ManagerManisDbProtocolSerializable.class, SerializableRpcEngine.class);

        final long version = RPC.getProtocolVersion(ManagerManisDbProtocolSerializable.class);
        int rpcTimeOut = 6000;
        ManagerManisDbProtocolSerializable proxy =
                RPC.getProtocolProxy(ManagerManisDbProtocolSerializable.class, version,
                        address, conf, SocketFactory.getDefault(), rpcTimeOut);
        return proxy;
    }

    private static ClientProtocol createManisDbProxyWithClientProtocol(Configuration conf,
                                                                       InetSocketAddress address)
            throws IOException {
        RPC.setProtocolEngine(conf, ClientManisDbProtocolPB.class, ProtobufRpcEngine.class);

        final long version = RPC.getProtocolVersion(ClientManisDbProtocolPB.class);
        int rpcTimeOut = 6000;
        ClientManisDbProtocolPB proxy =
                RPC.getProtocolProxy(ClientManisDbProtocolPB.class,
                        version, address, conf,
                        SocketFactory.getDefault(), rpcTimeOut);

        return new ClientManisDbProtocolTranslatorPB(proxy);
    }

    @SuppressWarnings("unchecked")
    public static <T> ProxyInfo<T> createProxy(Configuration conf,
        URI uri, Class<T> xface)
            throws IOException {
        InetSocketAddress manisDbAddr = ManisDb.getAddress(uri);

        T proxy;
        if (xface == ClientProtocol.class) {
            proxy =  (T) createManisDbProxyWithClientProtocol(conf, manisDbAddr);
        } else if (xface == ManagerProtocol.class) {
            proxy = (T) createManisDbProxyWithManagerProtocol(conf, manisDbAddr);
        } else {
            String message = "Unsupported protocol found when creating the proxy " +
                    "connection to ManisDb: " +
                    ((xface != null) ? xface.getName() : "null");
            throw new IllegalStateException(message);
        }

        return new ProxyInfo<T>(proxy, manisDbAddr);
    }
}
