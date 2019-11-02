package com.cnblogs.duma.server.manisdb;

import java.net.InetSocketAddress;
import java.net.URI;

/**
 * @author duma
 */
public class ManisDb {
    private static final String MANIS_URI_SCHEMA = "manis";
    private static final int DEFAULT_PORT = 8866;


    public static InetSocketAddress getAddress(String host) {
        return new InetSocketAddress(host, DEFAULT_PORT);
    }

    public static InetSocketAddress getAddress(URI manisDbUri) {
        String host = manisDbUri.getHost();
        if (host == null) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for ManisDB address: %s has no host.",
                    manisDbUri.toString()));
        }
        if (!MANIS_URI_SCHEMA.equalsIgnoreCase(manisDbUri.getScheme())) {
            throw new IllegalArgumentException(String.format(
                    "Invalid URI for NameNode address: %s is not of scheme '%s'.",
                    manisDbUri.toString(), MANIS_URI_SCHEMA));
        }

        return getAddress(host);
    }
}
