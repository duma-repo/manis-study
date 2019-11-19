package com.cnblogs.duma.util;

import java.lang.reflect.Constructor;

public class ReflectionUtils {
    private static final Class<?>[] EMPTY_ARRAY = new Class[]{};
    /**
     * 通过反射创建对象，调用的是类默认构造方法
     * @param theClass 需要实例化的类
     * @return 实例化的对象
     */
    @SuppressWarnings("unchecked")
    public static <T> T newInstance(Class<T> theClass) {
        T result;
        try {
            Constructor<T> meth = theClass.getDeclaredConstructor(EMPTY_ARRAY);
            meth.setAccessible(true);
            result = meth.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
