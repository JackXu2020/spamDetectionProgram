package rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Hashtable;

public class Service {
    public static ServerSocket serverSocket;
    private static int servicePort = 1689;

    public static void setServicePort(int port) {
        if (serverSocket == null) {
            servicePort = port;
        }
    }

    private static Hashtable<String, Object> bindService = new Hashtable<>();

    public static void bind(String bindName, Object bindObject) {
        if (serverSocket == null) {
            try {
                serverSocket = new ServerSocket(servicePort);
                ConnectThread ct = new ConnectThread(serverSocket);
                ct.start();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        bindService.put(bindName, bindObject);
    }

    public static String getBindObjectInterface(String bindName) {
        try {
            Class<?> nowClass = bindService.get(bindName).getClass();
            Class<?> interfaceClass = null;
            while (nowClass.getSuperclass() != null && interfaceClass == null) {
                if (nowClass.getInterfaces().length > 0) {
                    interfaceClass = nowClass.getInterfaces()[0];
                    break;
                }
                nowClass = nowClass.getSuperclass();
            }
            return interfaceClass.getCanonicalName();
        } catch (Exception e) {
            return "";
        }
    }

    public static Object getInvokeReturn(String bindName, String methodName,
                                         Class<?>[] argtypes, Object[] args) {
        try {
            Object obj = bindService.get(bindName);
            Method method = obj.getClass().getMethod(methodName, argtypes);
            Object returnObj = method.invoke(obj, args);
            return returnObj;
        } catch (NoSuchMethodException | IllegalAccessException|
                InvocationTargetException e) {
            e.printStackTrace();
            return null;
        }
    }

    public static Object lookup(String serverName, String bindName) {
        try {
            Socket client = new Socket(InetAddress.getByName(serverName),
                    servicePort);
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
            DataInputStream in = new DataInputStream(client.getInputStream());
            out.writeInt(ServiceFlag.SERVICE_GETINTERFACE);
            out.writeUTF(bindName);
            in.readInt();
            String interfaceName = in.readUTF();
            Class<?> interfaceClass = Class.forName(interfaceName);
            ServiceHandler handler = new ServiceHandler(servicePort, bindName,
                    serverName);
            Object proxyObj = Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                    new Class[] {interfaceClass}, handler);
            return proxyObj;
        } catch (IOException | ClassNotFoundException e){
            e.printStackTrace();
            return null;
        }

    }

}
