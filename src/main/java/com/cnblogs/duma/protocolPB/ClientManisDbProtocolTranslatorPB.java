package com.cnblogs.duma.protocolPB;

import com.cnblogs.duma.ipc.RPC;
import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.GetTableCountRequestProto;
import com.google.protobuf.ServiceException;

import java.io.Closeable;
import java.io.IOException;

public class ClientManisDbProtocolTranslatorPB implements
        ClientProtocol, Closeable {
    private ClientManisDbProtocolPB rpcProxy;

    public ClientManisDbProtocolTranslatorPB(ClientManisDbProtocolPB proxy) {
        rpcProxy = proxy;
    }

    @Override
    public int getTableCount(String dbName, String tbName) throws IOException {
        GetTableCountRequestProto request = GetTableCountRequestProto.newBuilder()
                .setDbName(dbName)
                .setTbName(tbName)
                .build();
        try {
            return rpcProxy.getTableCount(null, request).getResult();
        } catch (ServiceException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws IOException {
    }
}
