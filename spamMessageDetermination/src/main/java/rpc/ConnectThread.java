package rpc;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ConnectThread extends Thread {
    ServerSocket server;
    Executor executor = new ScheduledThreadPoolExecutor(50);

    public ConnectThread(ServerSocket server) {
        this.server = server;
    }

    public void run() {
        while (true) {
            try {
                Socket client = server.accept();
                ServiceThread st = new ServiceThread(client);
                executor.execute(st);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
