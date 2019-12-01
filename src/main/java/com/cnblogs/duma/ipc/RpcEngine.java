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

    /**
     * 返回一个 Server 实例
     * @param protocol 接口（协议）
     * @param instance 接口（协议）的实例
     * @param bindAddress 服务端地址
     * @param port 服务端端口
     * @param numHandlers handler 线程个数
     * @param numReaders reader 线程个数
     * @param queueSizePerHandler 每个 Handler 期望的消息队列大小
     * @param verbose 是否对调用信息打log
     * @param conf Configuration 对象
     * @return Server 实例
     * @throws IOException
     */
    RPC.Server getServer(Class<?> protocol, Object instance, String bindAddress,
                         int port, int numHandlers, int numReaders,
                         int queueSizePerHandler, boolean verbose,
                         Configuration conf) throws IOException;
}
