package rpc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.Socket;

public class ServiceHandler implements InvocationHandler {
    int port;
    String bindName;
    String serverName;

    public ServiceHandler(int port, String bindName, String serverName) {
        this.port = port;
        this.bindName = bindName;
        this.serverName = serverName;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Socket client = new Socket(InetAddress.getByName(serverName), port);
        DataOutputStream out = new DataOutputStream(client.getOutputStream());
        out.writeInt(ServiceFlag.SERVICE_GETINVOKERETURN);
        out.writeUTF(bindName);
        out.writeUTF(method.getName());
        ObjectOutputStream objOut = new ObjectOutputStream(client.getOutputStream());
        objOut.writeObject(method.getParameterTypes());
        objOut.writeObject(args);
        DataInputStream in = new DataInputStream(client.getInputStream());
        in.readInt();
        ObjectInputStream objIn = new ObjectInputStream(client.getInputStream());
        return objIn.readObject();
    }
}
