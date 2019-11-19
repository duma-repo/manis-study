package com.cnblogs.duma.ipc;

import java.io.IOException;

public class RpcException extends IOException {
    RpcException(final String message) {
        super(message);
    }

    RpcException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
