package rpc;

import java.io.*;
import java.net.Socket;

public class ServiceThread implements Runnable{
    
    Socket client;
    public ServiceThread(Socket client) {
        this.client = client;
    }
    
    public void run() {
        String bindName = "";
        try {
            DataInputStream in = new DataInputStream(client.getInputStream());
            DataOutputStream out = new DataOutputStream(client.getOutputStream());
            int command = in.readInt();
            switch (command) {
                case ServiceFlag.SERVICE_GETINTERFACE:
                    bindName = in.readUTF();
                    out.writeInt(ServiceFlag.SERVICE_GETINTERFACE);
                    out.writeUTF(Service.getBindObjectInterface(bindName));
                    break;
                case ServiceFlag.SERVICE_GETINVOKERETURN:
                    bindName = in.readUTF();
                    String methodName = in.readUTF();
                    ObjectInputStream objIn = new ObjectInputStream(
                            client.getInputStream()
                    );
                    Class<?>[] argtypes = (Class<?>[]) objIn.readObject();
                    Object[] args = (Object[]) objIn.readObject();
                    Object obj = Service.getInvokeReturn(bindName, methodName,
                            argtypes, args);
                    out = new DataOutputStream(client.getOutputStream());
                    out.writeInt(ServiceFlag.SERVICE_GETINVOKERETURN);
                    ObjectOutputStream objOut = new ObjectOutputStream(
                            client.getOutputStream()
                    );
                    objOut.writeObject(obj);
                    break;
            }
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }
}
