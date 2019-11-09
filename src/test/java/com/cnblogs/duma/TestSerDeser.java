package com.cnblogs.duma;

import com.cnblogs.duma.ipc.protobuf.ProtobufRpcEngineProtos.RequestHeaderProto;
import com.cnblogs.duma.protocol.proto.ClientManisDbProtocolProtos.GetTableCountRequestProto;
import com.google.protobuf.Message;
import org.junit.Test;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TestSerDeser {

    @Test
    public void testSerDeser() throws Exception {
        /* 加载 Invocation 类 */
        Class clazz = Class.forName("com.cnblogs.duma.ipc.SerializableRpcEngine$Invocation");
        /* 通过反射获取 Invocation 类的有参构造方法 */
        Constructor constructor = clazz.getConstructor(Method.class, Object[].class, long.class);
        /* 开放构造方法的权限 */
        constructor.setAccessible(true);
        /*
         * 创造 Invocation 有参构造方法第一个参数：Method 类型的实参，
         * 定义一个普通的方法 testFunc，并通过反射获取它
         */
        Method method = TestSerDeser.class.getMethod("testFunc", int.class, int.class);
        /* 创造 Invocation 有参构造方法第二个参数：Object数组类型的实参 */
        Object[] args = new Object[]{1, 2};
        /* 通过反射实例化 Invocation */
        Object object = constructor.newInstance(method, args, 1);
        System.out.println(object);

        /* 构造一个 DataOutputStream 对象 out 来模拟输出流 */
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        /* 通过反射获取 Invocation 的 write 方法 */
        Method methodWrite = clazz.getMethod("write", DataOutput.class);
        methodWrite.setAccessible(true);
        /* 通过反射调用 object 对象的 write 方法，实现序列化并将数据写入 out 中 */
        methodWrite.invoke(object, out);
        /* 输出序列化后的结果大小 */
        System.out.println(out.size());

        /* 拿到序列化的结果（bo.toByteArray），构造 DataInputStream 对象 in 来模拟输入流 */
        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);

        /* 通过反射获取 Invocation 类的无参构造方法 */
        Constructor defaultConstructor = clazz.getDeclaredConstructor();
        defaultConstructor.setAccessible(true);
        /* 通过反射实例化 Invocation 对象 obj2 */
        Object obj2 = defaultConstructor.newInstance();

        Method methodRead = clazz.getMethod("readFields", DataInput.class);
        methodRead.setAccessible(true);
        /* 通过反射调用 obj2 对象的 readFields 方法，从输入流 in 中反序列化 */
        methodRead.invoke(obj2, in);

        System.out.println(obj2);
    }

    public void testFunc(int a, int b) {

    }

    @Test
    public void testRpcRequestWrapper() throws Exception {
        RequestHeaderProto header = RequestHeaderProto.newBuilder()
                .setMethodName("testRpcRequestWrapper")
                .setDeclaringClassProtocolName("testProtocolName")
                .setClientProtocolVersion(1)
                .build();
        GetTableCountRequestProto req = GetTableCountRequestProto.newBuilder()
                .setDbName("db1")
                .setTbName("tb1")
                .build();

        Class clazz = Class.forName("com.cnblogs.duma.ipc.ProtobufRpcEngine$RpcRequestWrapper");
        Constructor constructor = clazz.getConstructor(RequestHeaderProto.class, Message.class);
        constructor.setAccessible(true);
        Object rpcReqWrapper = constructor.newInstance(header, req);
        System.out.println(rpcReqWrapper);

        /*
         * 测试序列化方法 write
         */
        ByteArrayOutputStream bo = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bo);
        Method writeMethod = clazz.getMethod("write", DataOutput.class);
        writeMethod.setAccessible(true);
        writeMethod.invoke(rpcReqWrapper, out);
        System.out.println("write size: " + bo.toByteArray().length);

        /*
         * 测试反序列化方法 readFields
         */
        Constructor defaultConstructor = clazz.getConstructor();
        defaultConstructor.setAccessible(true);
        Object newRpcReqWrapper = defaultConstructor.newInstance();

        Field theRequestReadField = clazz.getSuperclass().getDeclaredField("theRequestRead");
        theRequestReadField.setAccessible(true);
        byte[] theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        assert theRequestRead == null;

        ByteArrayInputStream baInputStream = new ByteArrayInputStream(bo.toByteArray());
        DataInput in = new DataInputStream(baInputStream);
        Method readFieldsMethod = clazz.getMethod("readFields", DataInput.class);
        readFieldsMethod.setAccessible(true);
        readFieldsMethod.invoke(newRpcReqWrapper, in);

        /*
         * 使用断言，判断 requestHeader 字段反序列化结果
         */
        assert newRpcReqWrapper.toString().equals(rpcReqWrapper.toString());

        /*
         * 使用断言，判断 theRequestRead 字段反序列化
         */
        theRequestRead = (byte[]) theRequestReadField.get(newRpcReqWrapper);
        GetTableCountRequestProto deSerReq = GetTableCountRequestProto.parseFrom(theRequestRead);
        assert deSerReq.getDbName().equals("db1");
        assert deSerReq.getTbName().equals("tb1");
    }
}
