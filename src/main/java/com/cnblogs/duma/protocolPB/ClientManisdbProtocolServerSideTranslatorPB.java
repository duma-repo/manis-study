package com.cnblogs.duma.protocolPB;

import com.cnblogs.duma.protocol.ClientProtocol;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.GetTableCountRequestProto;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.GetTableCountResponseProto;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;

/**
 * 该类在服务端使用 {@link ClientManisDbProtocolPB}
 * 它将 PB 数据类型转到 ClientProtocol 接口中定义的数据类型
 * @author duma
 */
public class ClientManisdbProtocolServerSideTranslatorPB
        implements ClientManisDbProtocolPB {

    final private ClientProtocol server;

    public ClientManisdbProtocolServerSideTranslatorPB(ClientProtocol server) {
        this.server = server;
    }

    @Override
    public GetTableCountResponseProto getTableCount(
            RpcController controller,
            GetTableCountRequestProto request)
            throws ServiceException {
        String dbName = request.getDbName();
        String tbName = request.getTbName();
        try {
            int ret = server.getTableCount(dbName, tbName);
            return GetTableCountResponseProto.newBuilder().setResult(ret).build();
        } catch (IOException e) {
            throw new ServiceException(e);
        }
    }
}
