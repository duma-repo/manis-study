package com.cnblogs.duma.util;

import com.cnblogs.duma.ipc.RPC;
import com.google.protobuf.ByteString;

import java.io.DataInput;
import java.io.IOException;

/**
 * 提供 Protocol Buffer 相关的工具方法
 * @author duma
 */
public class ProtoUtil {
    /**
     * 从输入流中读取边长的 int 类型变量
     * Protocol Buffer writeDelimitedTo 写入的长度使用该编码方式
     * @param in 输入流
     * @return 解码结果
     * @throws IOException 编码格式错误或者EOF.
     */
    public static int readRawVarInt32(DataInput in) throws IOException {
        byte tmp = in.readByte();
        if (tmp >= 0) {
            return tmp;
        }
        int result = tmp & 0x7f;
        if ((tmp = in.readByte()) >= 0) {
            result |= tmp << 7;
        } else {
            result |= (tmp & 0x7f) << 7;
            if ((tmp = in.readByte()) >= 0) {
                result |= tmp << 14;
            } else {
                result |= (tmp & 0x7f) << 14;
                if ((tmp = in.readByte()) >= 0) {
                    result |= tmp << 21;
                } else {
                    result |= (tmp & 0x7f) << 21;
                    result |= (tmp = in.readByte()) << 28;
                    if (tmp < 0) {
                        // varInt32 最多使用 5 位编码，最后一位仍然小于 0， 说明有问题
                        for (int i = 0; i < 5; i++) {
                            if (in.readByte() >= 0) {
                                return result;
                            }
                        }
                        throw new IOException("Malformed varint");
                    }
                }
            }
        }
        return result;
    }
}
