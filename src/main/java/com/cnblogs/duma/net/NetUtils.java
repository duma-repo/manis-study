package com.cnblogs.duma.net;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

public class NetUtils {
    private static final Log LOG = LogFactory.getLog(NetUtils.class);

    /**
     * {@link Socket#connect(SocketAddress, int)} 的替代函数
     * 调用 <code>socket.connect(endpoint, timeout)</code> 进行连接
     * Hadoop 中有自定义的 SocketFactory 它的连接方式与这里略有不同，
     * 并且为了获得连接的控制权 Hadoop 还自定义了 selector
     *
     * @param socket socket
     * @param endPoint 远程地址
     * @param timeout 连接超时时间，单位：ms
     * @throws IOException
     */
    public static void connect(Socket socket,
                               InetSocketAddress endPoint, int timeout) throws IOException {
        if (socket == null || endPoint == null || timeout < 0) {
            throw new IllegalArgumentException("Illegal Argument for connect()");
        }
        socket.connect(endPoint, timeout);

        /**
         * TCP 会出现一种罕见的情况 —— 自连接。
         * 在服务端 down 掉的情况下，客户端连接本地的服务端有可能连接成功
         * 且客户端的地址和端口与服务端相同。这会导致本地再次启动服务端失败，因为端口和地址被占用
         * 因此，出现这种情况视为连接拒绝
         */
        if (socket.getLocalAddress().equals(socket.getInetAddress()) &&
            socket.getLocalPort() == socket.getPort()) {
            LOG.info("Detected a loopback TCP socket, disconnecting it");
            socket.close();
            throw new ConnectException("Localhost targeted connection resulted in a loopback. " +
                    "No daemon is listening on the target port."
            );
        }
    }

    public static InputStream getInputStream(Socket socket) throws IOException {
        return socket.getInputStream();
    }

    public static OutputStream getOutputStream(Socket socket) throws IOException {
        return socket.getOutputStream();
    }
}
