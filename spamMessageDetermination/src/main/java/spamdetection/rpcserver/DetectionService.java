package spamdetection.rpcserver;

import rpc.Service;
import spamdetection.rpcserver.bizimpl.SpamDetectionBizimpl;
import spamdetection.rpcserver.bizinterface.SpamDetectionBizInterface;

public class DetectionService {

    public static void main(String[] args) {
        SpamDetectionBizInterface biz = new SpamDetectionBizimpl();
        Service.bind("service", biz);
    }
}
