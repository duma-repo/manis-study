package com.cnblogs.duma.ipc;

import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * @author duma
 */
public abstract class Server {
    public static final Log LOG = LogFactory.getLog(Server.class);
    private String bindAddress;
    private int port;
    private int handlerCount;
    private int readThreads;

    volatile private boolean running = true;

    private Configuration conf;

    /**
     * Server 类构造方法
     * @param bindAddress 服务端地址
     * @param port 服务端端口
     * @param numHandlers Handler 线程的数量
     * @param numReaders Reader 线程的数量
     * @param queueSizePerHandler 每个 Handler 期望的消息队列大小，再根据 numHandlers 可以得出队列总大小
     * @param conf 配置
     * @throws IOException
     */
    protected Server(String bindAddress, int port,
                     int numHandlers, int numReaders, int queueSizePerHandler,
                     Configuration conf) throws IOException {
    }

    /**
     * 启动服务
     */
    public synchronized void start() {

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
}
