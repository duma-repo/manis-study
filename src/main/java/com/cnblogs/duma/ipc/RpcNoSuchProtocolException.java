package com.cnblogs.duma.ipc;

import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;

/**
 * 当客户端请求的接口（协议）在服务端找不到是抛出此异常
 * @author duma
 */
public class RpcNoSuchProtocolException extends RpcServerException {
    public RpcNoSuchProtocolException(final String message) {
        super(message);
    }

    @Override
    public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.ERROR;
    }

    @Override
    public RpcErrorCodeProto getRpcErrorCodeProto() {
        return RpcErrorCodeProto.ERROR_NO_SUCH_PROTOCOL;
    }
}
