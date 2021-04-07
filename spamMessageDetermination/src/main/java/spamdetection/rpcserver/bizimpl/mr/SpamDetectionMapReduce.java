package spamdetection.rpcserver.bizimpl.mr;

import hadoop.hdfs.HdfsOps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import redis.clients.jedis.Jedis;
import redis.util.RedisPool;
import rpc.Service;
import spamdetection.rpcserver.ServerContext;
import spamdetection.rpcserver.WordInfo;
import spamdetection.rpcserver.bizinterface.SpamDetectionBizInterface;

import java.io.IOException;



public class SpamDetectionMapReduce {

    public void beginMR() throws Exception {

        SpamDetectionBizInterface biz = (SpamDetectionBizInterface) Service.lookup(
                ServerContext.COUNTER_SERVER, "service"
        );

        biz.setGlobalCounterValue("CounterSpam", 0);
        biz.setGlobalCounterValue("CounterHam", 0);
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf,
                new String[]{
                        ServerContext.HDFS_ROOT + "/spamDetection/learningData",
                        ServerContext.HDFS_ROOT + "/spamDetection/output"
                }).getRemainingArgs();
        System.out.println(otherArgs[0] + " " + otherArgs[1]);
        Job job = Job.getInstance(conf, "wc");
        job.setJarByClass(SpamDetectionMapReduce.class);
        job.setMapperClass(SpamDetectionMapper.class);
        job.setReducerClass(SpamDetectionReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        HdfsOps ops = new HdfsOps(ServerContext.HDFS_ROOT);
        ops.deleteFile(ServerContext.HDFS_ROOT + "/spamDetection/output");
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        if (!job.waitForCompletion(true)) {
            return;
        }
    }

    public static class SpamDetectionMapper extends
            Mapper<Object, Text, Text, Text> {
        SpamDetectionBizInterface biz = (SpamDetectionBizInterface) Service.lookup(
                ServerContext.COUNTER_SERVER, "service"
        );
        Text ham = new Text("ham");
        Text spam = new Text("spam");

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String data = value.toString();
            String[] totalWords = data.split("\t");
            String[] words = totalWords[1].split(" ");
            if ("ham".equals(totalWords[0])) {
                biz.globalCounterValueIncrement("CounterHam", 1);

                for (String word: words) {
                    context.write(new Text(word), ham);
                }
            } else {
                //context.getCounter(MsgCounter.CounterSpam).increment(1);
                biz.globalCounterValueIncrement("CounterSpam", 1);
                for (String word: words) {
                    context.write(new Text(word), spam);
                }
            }
        }
    }

    public static class SpamDetectionReducer extends
            Reducer<Text, Text, Text, Text> {
        long spamNum = -1;
        long hamNum = -1;
        SpamDetectionBizInterface biz = (SpamDetectionBizInterface) Service.lookup(
                ServerContext.COUNTER_SERVER, "service"
        );

        public void reduce(Text key, Iterable<Text> values, Context context) {
            if (spamNum == -1 || hamNum == -1) {
                spamNum = biz.getGlobalCounterValue("CounterSpam");
                hamNum = biz.getGlobalCounterValue("CounterHam");
                System.out.println(
                        "valid message number: " + hamNum + " , junk message number: "
                                + spamNum);
            }
            Jedis jedis = RedisPool.getJedis(false);
            WordInfo wordInfo = new WordInfo();
            wordInfo.setWord(key.toString());
            for (Text value: values) {
                if ("ham".equals(value.toString())) {
                    wordInfo.setHamNum(wordInfo.getHamNum() + 1);
                } else {
                    wordInfo.setSpamNum(wordInfo.getSpamNum() + 1);
                }
            }
            wordInfo.setWordHamPossibility(
                    computeWordHamPossibility(wordInfo.getHamNum()));
            wordInfo.setWordSpamPossibility(
                    computeWordSpamPossibility(wordInfo.getSpamNum())
            );

            try {
                jedis.set(key.toString().getBytes(),
                        wordInfo.saveInstanceToByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }
            RedisPool.returnResource(jedis);
        }

        float computeWordHamPossibility(int wordHamNum) {

            // 计算贝叶斯分类概率，+1:拉普拉斯平滑处理
            float result = ((float) wordHamNum / (float) (hamNum + 1))
                    * ((float) (hamNum + 1) / (float) (hamNum + spamNum + 1))
                    / (((float) wordHamNum + 1) / (float) (hamNum + spamNum + 1));
            // 返回计算结果
            return result;
        }

        float computeWordSpamPossibility(int wordSpamNum) {

            // 计算贝叶斯分类概率，+1:拉普拉斯平滑处理
            float result = ((float) wordSpamNum / (float) (spamNum + 1))
                    * ((float) (spamNum + 1) / (float) (hamNum + spamNum + 1))
                    / (((float) wordSpamNum + 1) / (float) (hamNum + spamNum + 1));
            // 返回计算结果
            return result;

        }

    }

    public static void main(String[] args) throws Exception {
        SpamDetectionMapReduce mr = new SpamDetectionMapReduce();
        mr.beginMR();
        SpamDetectionBizInterface biz = (SpamDetectionBizInterface) Service
                .lookup(ServerContext.COUNTER_SERVER, "service");
        System.out.println(biz.isSpam("Free free free"));
    }
}
