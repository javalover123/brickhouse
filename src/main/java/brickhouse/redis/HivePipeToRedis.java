package brickhouse.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.util.JedisURIHelper;

import java.net.URI;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class HivePipeToRedis {

    public static final Logger LOGGER = LoggerFactory.getLogger(HivePipeToRedis.class);

    public static Map<String, String> map;

    public static void main(String[] args) {
        if (args == null || args.length == 0) {
            LOGGER.error("参数不对,格式如：java -jar *.jar redis的URI 批次大小");
            System.exit(1);
        }

        List<String> parameters = Arrays.asList(args);
        LOGGER.info("start," + parameters);

        String host = args[0];
        long batch = args.length > 1 ? Long.parseLong(args[1]) : 10000;

        long total = 0;
        long beginTime = System.currentTimeMillis();
        map = new LinkedHashMap<>((int) (batch / 0.7));
        URI uri = URI.create(host);
        HostAndPort hostAndPort = new HostAndPort(uri.getHost(), uri.getPort());
        String password = JedisURIHelper.getPassword(uri);
        JedisCluster jedisCluster = JedisClusterUtil.getJedisCluster(hostAndPort, password);

        Scanner sc = new Scanner(System.in);
        while (sc.hasNext()) {
            String line = sc.nextLine();
            // System.out.println(line);
            int index = line.indexOf('\t');
            if (index < 0) {
                LOGGER.warn("无效数据," + line);
                continue;
            }

            String key = line.substring(0, index);
            String value = line.substring(index + 1);
            LOGGER.debug("key," + key);
            map.put(key, value);
            total++;
            if (map.size() >= batch) {
                batchUpdate(jedisCluster, map);
            }
        }

        batchUpdate(jedisCluster, map);
        long cost = System.currentTimeMillis() - beginTime;
        LOGGER.info("end," + parameters + "," + total + "," + TimeUnit.MILLISECONDS.toMinutes(cost));
        System.exit(0);
    }

    protected static void batchUpdate(JedisCluster jedisCluster, Map<String, String> setList) {
        if (setList.isEmpty()) {
            return;
        }
        try (JedisClusterPipeline pipeline = JedisClusterPipeline.pipelined(jedisCluster)) {
            for (Map.Entry<String, String> entry : setList.entrySet()) {
                pipeline.set(entry.getKey(), entry.getValue());
            }
            pipeline.sync();
            LOGGER.info(" Doing Batch Set " + setList.size() + " records;");
            setList.clear();
        } catch (Exception e) {
            throw e;
        }
    }

}
