package com.cnblogs.duma.io;

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;

/**
 * OutputStream 的实现，包装了 DataOutput
 * @author duma
 */
public class DataOutputOutputStream extends OutputStream {
    private final DataOutput out;

    /**
     * 从 DataOutput 对象中构造 OutputStream 对象
     * 如果 'out' 已经是 OutputStream 对象, 直接返回。否则，new DataOutputOutputStream 对象
     * @param out the DataOutput to wrap
     * @return an OutputStream instance that outputs to 'out'
     */
    public static OutputStream constructDataOutputStream(DataOutput out) {
        if (out instanceof OutputStream) {
            return (OutputStream)out;
        }
        return new DataOutputOutputStream(out);
    }

    private DataOutputOutputStream(DataOutput out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.writeByte(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        out.write(b, off, len);
    }
}
