package com.cnblogs.duma.io;

import org.apache.commons.logging.Log;

import java.io.Closeable;
import java.io.IOException;

public class IOUtils {


    public static void cleanup(Log log, Closeable... closeables) {
        for (Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing " + c, e);
                    }
                }
            }
        }
    }

    /**
     * 关闭数据流
     * @param stream 待关闭的数据流
     */
    public static void closeStream(Closeable stream) {
        cleanup(null, stream);
    }
}
