package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.*;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;

/**
 * @author duma
 */
public class SerializableRpcEngine implements RpcEngine {

    private static class Invoker implements RpcInvocationHandler {

        private Invoker(Class<?> protocol, InetSocketAddress address,
                        Configuration conf, SocketFactory factory,
                        int rpcTimeOut)
                throws IOException {
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            return null;
        }

        @Override
        public void close() throws IOException {
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
}
