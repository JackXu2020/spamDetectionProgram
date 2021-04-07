package redis.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import spamdetection.rpcserver.ServerContext;

public class RedisPool {
    private static String address = ServerContext.REDIS_SERVER;
    private static int port = Integer.parseInt(ServerContext.REDIS_PORT);
    private static String AUTH = ServerContext.REDIS_PASS;
    private static int MAX_ACTIVE = 1024;
    private static int MAX_IDLE = 200;
    private static long MAX_WAIT = 10000;
    private static int TIMEOUT = 10000;
    // 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的
    private static boolean TEST_ON_BORROW = true;

    private static JedisPool jedisPool = null;
    private static JedisPoolConfig config = null;

    static {
        try {
            config = new JedisPoolConfig();
            config.setMaxTotal(MAX_ACTIVE);
            config.setMaxIdle(MAX_IDLE);
            config.setMaxWaitMillis(MAX_WAIT);
            config.setTestOnBorrow(TEST_ON_BORROW);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized static Jedis getJedis(boolean auth) {
        try {
            if (jedisPool == null) {
                if (auth) {
                    jedisPool = new JedisPool(config, address, port, TIMEOUT, AUTH);
                } else {
                    jedisPool = new JedisPool(config, address, port, TIMEOUT);
                }
            }

            Jedis resource = jedisPool.getResource();
            return resource;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void returnResource(final Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }


    public static void main(String[] args) {
        Jedis jedis = getJedis(false);
        System.out.println(jedis.get("k1"));
    }

}
