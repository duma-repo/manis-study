package com.cnblogs.duma.ipc;

public class RpcClientException extends RpcException {

    RpcClientException(String message) {
        super(message);
    }

    RpcClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
