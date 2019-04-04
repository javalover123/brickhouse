package brickhouse.redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.time.LocalDateTime;

public class JedisClusterUtil {

    private static GenericObjectPoolConfig initPoolConfiguration() {
        System.out.println(Thread.currentThread().getName() + "======初始化REDIS连接池============" + LocalDateTime.now());
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();

        config.setLifo(true);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(false);
        config.setBlockWhenExhausted(true);
        // config.setMinIdle(Math.max(1, (int) (poolSize / 10))); // keep 10% hot always
        config.setMinIdle(5);
        config.setMaxTotal(500); // the number of Jedis connections to create
        config.setTestWhileIdle(false);
        config.setSoftMinEvictableIdleTimeMillis(3000L);
        config.setNumTestsPerEvictionRun(5);
        config.setTimeBetweenEvictionRunsMillis(5000L);
        config.setJmxEnabled(false);
        config.setMaxWaitMillis(5000);
//        config.setJmxEnabled(true);

        return config;
    }

    //单例，保证一个map jvm里面只有一个jedisCluster实例，这样所有分到这个map container里面的数据共享这一个jedisCluster实例，比在evaluate每次创建jedisCluster性能要好一点
    private static volatile JedisCluster jedisCluster = null;

    public static synchronized JedisCluster getJedisCluster(HostAndPort hostAndPort, String password) {
        if (jedisCluster == null) {
            jedisCluster = new JedisCluster(hostAndPort, 10000, 10000, 3, password, initPoolConfiguration());

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                // Clean up at exit
                @Override
                public void run() {
                    System.out.println(JedisClusterUtil.class.getSimpleName() + " shutdown");

                    try {
                        if (jedisCluster != null) {
                            jedisCluster.close();
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        jedisCluster = null;
                    }
                }
            }));
        }
        return jedisCluster;
    }
}
