package com.cnblogs.duma.server.manisdb;

import com.cnblogs.duma.conf.Configuration;
import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.ipc.SerializableRpcEngine;

public class ManisDbUtil {
    public static void addSeriablizableProtocol(Configuration conf
            , Class<?> protocol, Object service, RPC.Server server) {
        RPC.setProtocolEngine(conf, protocol, SerializableRpcEngine.class);
        server.addProtocol(RPC.RpcKind.RPC_SERIALIZABLE, protocol, service);
    }
}
