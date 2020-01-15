package com.cnblogs.duma.server.manisdb;

import com.cnblogs.duma.conf.CommonConfigurationKeysPublic;
import com.cnblogs.duma.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author duma
 */
public class ManisDb {
    public static final Log LOG = LogFactory.getLog(ManisDb.class.getName());
    private static final String MANIS_URI_SCHEMA = "manis";
    private static final int DEFAULT_PORT = 8866;
    private static final String DEFAULT_URI = "uri://";

    private ManisDbRpcServer rpcServer;

    public static InetSocketAddress getAddress(String host) {
        return new InetSocketAddress(host, DEFAULT_PORT);
    }

    public static InetSocketAddress getAddress(URI manisDbUri) {
        String host = manisDbUri.getHost();
        if (host == null) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for ManisDB address: %s has no host.",
                    manisDbUri.toString()));
        }
        if (!MANIS_URI_SCHEMA.equalsIgnoreCase(manisDbUri.getScheme())) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for NameNode address: %s is not of scheme '%s'.",
                    manisDbUri.toString(), MANIS_URI_SCHEMA));
        }

        return getAddress(host);
    }

    private static URI getDefaultUri(Configuration conf, String key) {
        return URI.create(conf.get(key, DEFAULT_URI));
    }

    protected static InetSocketAddress getProtoBufRpcServerAddress(Configuration conf) {
        URI uri = getDefaultUri(conf, CommonConfigurationKeysPublic.MANIS_RPC_PROTOBUF_KEY);
        return getAddress(uri);
    }

    public ManisDb(Configuration conf) throws IOException {
        init(conf);
    }

    /**
     * 初始化 ManisDb
     */
    void init(Configuration conf) throws IOException {
        rpcServer = createRpcServer(conf);
        startServices();
    }

    private void startServices() {
        rpcServer.start();
    }

    public void join() {
        try {
            rpcServer.join();
        } catch (InterruptedException ie) {
            LOG.info("Caught interrupted exception ", ie);
        }
    }

    ManisDbRpcServer createRpcServer(Configuration conf) throws IOException {
        return new ManisDbRpcServer(conf);
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();

        ManisDb manisDb = new ManisDb(conf);
        manisDb.join();
    }
}
