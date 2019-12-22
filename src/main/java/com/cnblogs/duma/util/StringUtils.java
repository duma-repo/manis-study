package com.cnblogs.duma.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class StringUtils {
    /**
     * 将 byte 数组转成 16 进制字符串
     * @param bytes 源数组
     * @param start 起始位置
     * @param end 截止位置
     * @return 16进制字符串
     */
    public static String byteToHexString(byte[] bytes, int start, int end) {
        if (bytes == null) {
            throw  new IllegalArgumentException("bytes == null");
        }
        StringBuilder s = new StringBuilder();
        for (int i = start; i < end; i++) {
            s.append(String.format("%02x", bytes[i]));
        }
        return s.toString();
    }

    public static String byteToHexString(byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    public static String stringifyException(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter printWriter = new PrintWriter(sw);
        e.printStackTrace(printWriter);
        printWriter.close();
        return sw.toString();
    }
}
