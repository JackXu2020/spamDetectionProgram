package spamdetection.rpcserver.bizimpl;

import hadoop.hdfs.HdfsOps;
import redis.clients.jedis.Jedis;
import redis.util.RedisPool;
import spamdetection.rpcserver.ServerContext;
import spamdetection.rpcserver.WordInfo;
import spamdetection.rpcserver.bizimpl.mr.GlobalCounters;
import spamdetection.rpcserver.bizimpl.mr.SpamDetectionMapReduce;
import spamdetection.rpcserver.bizinterface.SpamDetectionBizInterface;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;

public class SpamDetectionBizimpl implements SpamDetectionBizInterface {

    long FILE_LENGTH = 1024*1024*128;
    Jedis jedis = null;
    SpamDetectionMapReduce mr = new SpamDetectionMapReduce();
    @Override
    public void submitMsg(String msg, boolean isSpam) {

        File learningData = new File("learningData");
        if (learningData.exists() && learningData.length() > FILE_LENGTH) {
            moveLearningDataToHDFS(learningData);
            reMR();
        }
        try {
            FileOutputStream out = new FileOutputStream(learningData, true);
            String context = "";
            if (isSpam) {
                context += "spam" + "\t";
            } else {
                context += "ham"  + "\t";
            }
            context += msg;
            System.out.println("increase new learning data :" + context);
            out.write((context + "\n").getBytes());
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public boolean isSpam(String msg) {
        jedis = RedisPool.getJedis(false);
        String[] words = msg.split(" ");
        boolean result = computeStringSpamResult(words) >
                computeStringHamResult(words);
        return result;
    }

    float computeStringHamResult(String[] words) {
        float result = 1.0f;
        for (String word: words) {
            try {
                if (jedis.get(word.getBytes()) != null) {
                    WordInfo info = WordInfo.getInstanceByByteArray(
                            jedis.get(word.getBytes())
                    );
                    result *= info.getWordHamPossibility();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    float computeStringSpamResult(String[] words) {
        float result = 1.0f;
        for (String word: words) {
            try{
                if (jedis.get(word.getBytes()) != null) {
                    WordInfo info = WordInfo.getInstanceByByteArray(
                            jedis.get(word.getBytes())
                    );
                    result *= info.getWordSpamPossibility();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Override
    public void reMR() {
        try {
            mr.beginMR();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private void moveLearningDataToHDFS(File file) {
        String localPath = file.getAbsolutePath();
        String uuid = UUID.randomUUID().toString();
        try {
            HdfsOps hdfsOps = new HdfsOps(ServerContext.HDFS_ROOT);
            hdfsOps.uploadFile(localPath,
                    "/spamDetection/learningData/"
            +uuid +".txt");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public long getGlobalCounterValue(String counterKey) {
        return GlobalCounters.getCounterValue(counterKey);
    }

    @Override
    public void setGlobalCounterValue(String counterKey, long counterValue) {
        GlobalCounters.setCounterValue(counterKey, counterValue);
    }

    @Override
    public void globalCounterValueIncrement(String counterKey, long counterValue) {
        GlobalCounters.increment(counterKey, counterValue);
    }

    public static void main(String[] args) {
        SpamDetectionBizInterface biz = new SpamDetectionBizimpl();
        biz.reMR();
        System.out.println(biz.isSpam("Free Free Free"));
        System.out.println(biz.isSpam("hello wuzy"));
    }
}
