package com.cnblogs.duma.ipc;

import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcErrorCodeProto;
import com.cnblogs.duma.ipc.protobuf.RpcHeaderProtos.RpcResponseHeaderProto.RpcStatusProto;

import java.io.IOException;

/**
 * 继承 {@link java.io.IOException}，封装服务端状态的信息
 * @author duma
 */
public class RpcServerException extends IOException {
    public RpcServerException(String message) {
        super(message);
    }

    public RpcServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public RpcStatusProto getRpcStatusProto() {
        return RpcStatusProto.ERROR;
    }

    public RpcErrorCodeProto getRpcErrorCodeProto() {
        return RpcErrorCodeProto.ERROR_RPC_SERVER;
    }
}
