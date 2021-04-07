package MRSEClient.client;

import rpc.Service;
import spamdetection.rpcserver.bizinterface.SpamDetectionBizInterface;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Client {

    public static void main(String[] args) throws IOException {
        SpamDetectionBizInterface biz = (SpamDetectionBizInterface) Service.lookup(
                ServerContext.COUNTER_SERVER, "service"
        );
        biz.reMR();
        System.out.println(biz.isSpam("free free free."));
        InputStreamReader reader = new InputStreamReader(System.in);
        BufferedReader buffer = new BufferedReader(reader);
        while (true) {
            System.out.println(biz.isSpam(buffer.readLine()));
        }
    }
}
