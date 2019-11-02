package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * 实现 RPC 的接口
 * @author duma
 */
public interface RpcEngine {

    /**
     * 获取客户端的代理对象
     * @param protocol 需要代理的接口
     * @param clientVersion 客户端版本
     * @param address 服务端地址
     * @param conf 配置
     * @param factory 创建socket的工厂对象
     * @param rpcTimeOut rpc超时时间
     * @param <T> 泛型
     * @return 接口的代理对象
     * @throws IOException
     */
    <T> T getProxy(Class<T> protocol,
                   long clientVersion,
                   InetSocketAddress address,
                   Configuration conf,
                   SocketFactory factory,
                   int rpcTimeOut) throws IOException;
}
