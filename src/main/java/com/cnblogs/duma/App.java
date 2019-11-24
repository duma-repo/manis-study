package com.cnblogs.duma;

import com.cnblogs.duma.conf.Configuration;

import java.io.IOException;
import java.net.URI;

public class App {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Manager manager = new Manager(URI.create("manis://localhost:8866"), conf);
        boolean res = manager.setMaxTable(10);
        System.out.println(res);
        manager.close();

        ManisClient manisClient = new ManisClient(URI.create("manis://localhost:8866"), conf);
        int tableCount = manisClient.getTableCount("db1", "tb2");
        System.out.println(tableCount);
        manisClient.close();
    }
}