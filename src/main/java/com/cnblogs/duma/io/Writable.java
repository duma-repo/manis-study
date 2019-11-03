package com.cnblogs.duma.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author duma
 */
public interface Writable {
    /**
     * 对象的属性序列化后写入 out
     * @param out
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;

    /**
     * 从 <code>in</code> 中反序列化对象的属性
     * @param in
     * @throws IOException
     */
    void readFields(DataInput in) throws IOException;
}
