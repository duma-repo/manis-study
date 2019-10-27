package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.protocol.ManagerProtocol;

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

    @SuppressWarnings("unchecked")
    public static <T> ProxyInfo<T> createProxy(Configuration conf,
        URI uri, Class<T> xface)
            throws IOException {
        return null;
    }
}
