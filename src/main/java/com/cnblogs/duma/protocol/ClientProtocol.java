package com.cnblogs.duma.protocol;

import java.io.IOException;

public interface ClientProtocol {

    /**
     * 获取某个表的 meta 信息
     * @param dbName 数据库名称
     * @param tbName 表名称
     * @return 表中的记录数
     */
    public int getTableCount(String dbName, String tbName) throws IOException;
}
