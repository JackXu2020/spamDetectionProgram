package spamdetection.rpcserver;

import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class ServerContext {
    public static final String HDFS_ROOT;
    public static final String REDIS_SERVER;
    public static final String REDIS_PORT;
    public static final String REDIS_PASS;
    public static final String COUNTER_SERVER;

    static {
        PropertyResourceBundle bundle = (PropertyResourceBundle) ResourceBundle
                .getBundle("ServerContext");
        HDFS_ROOT = "hdfs://localhost:9000";
        REDIS_SERVER = "localhost";
        REDIS_PORT = "6379";
        REDIS_PASS = "admin";
        COUNTER_SERVER = "localhost";
    }
}
