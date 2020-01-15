package com.cnblogs.duma.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author duma
 */
public class Configuration {
    Properties properties;
    public static final String TRUE_STR = "true";
    public static final String FALSE_STR = "false";

    public Configuration() throws IOException {
        InputStream inStream = ClassLoader.getSystemResourceAsStream("manis-db.properties");
        properties = new Properties();
        properties.load(inStream);
    }

    public void set(String name, String value) {
        properties.setProperty(name, value);
    }

    public String get(String name) {
        return properties.getProperty(name);
    }

    public String get(String name, String defaultValue) {
        return properties.getProperty(name, defaultValue);
    }

    public int getInt(String name, int defaultValue) {
        String valueStr = get(name);
        if (valueStr == null) {
            return defaultValue;
        }
        return Integer.parseInt(valueStr);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        String valueStr = get(name);
        if (valueStr == null) {
            return defaultValue;
        }

        valueStr = valueStr.toLowerCase();
        if (TRUE_STR.equals(valueStr)) {
            return true;
        } else if (FALSE_STR.equals(valueStr)) {
            return false;
        } else {
            return defaultValue;
        }
    }

    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        if (!xface.isAssignableFrom(theClass)) {
            throw new RuntimeException(theClass + " not " + xface.getName());
        }

        set(name, theClass.getName());
    }

    public Class<?> getClassByName(String clsName) throws ClassNotFoundException {
        return Class.forName(clsName);
    }

    public Class<?> getClass(String name, Class<?> defaultValue) {
        String className = get(name);
        if (className == null) {
            return defaultValue;
        }

        try {
            return getClassByName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
