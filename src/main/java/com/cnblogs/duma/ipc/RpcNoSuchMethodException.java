package com.cnblogs.duma.ipc;

import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.*;

/**
 * 当客户端请求的方法不存在时，抛出此异常
 * @author duma
 */
public class RpcNoSuchMethodException extends RpcServerException {
    public RpcNoSuchMethodException(final String message) {
        super(message);
    }

    @Override
    public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.ERROR;
    }

    @Override
    public RpcErrorCodeProto getRpcErrorCodeProto() {
        return RpcErrorCodeProto.ERROR_NO_SUCH_METHOD;
    }
}
