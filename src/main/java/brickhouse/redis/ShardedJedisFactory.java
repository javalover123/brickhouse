package brickhouse.redis;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.util.Hashing;

import java.util.List;
import java.util.regex.Pattern;

public class ShardedJedisFactory implements PooledObjectFactory<ShardedJedis> {
    private List<JedisShardInfo> shards;
    private Hashing algo;
    private Pattern keyTagPattern;

    public ShardedJedisFactory(List<JedisShardInfo> shards, Hashing algo, Pattern keyTagPattern) {
        this.shards = shards;
        this.algo = algo;
        this.keyTagPattern = keyTagPattern;
    }

    @Override
    public PooledObject<ShardedJedis> makeObject() throws Exception {
        ShardedJedis jedis = new ShardedJedis(shards, algo, keyTagPattern);
        return new DefaultPooledObject<ShardedJedis>(jedis);
    }

    @Override
    public void destroyObject(PooledObject<ShardedJedis> pooledShardedJedis) throws Exception {
        final ShardedJedis shardedJedis = pooledShardedJedis.getObject();
        for (Jedis jedis : shardedJedis.getAllShards()) {
            if (jedis.isConnected()) {
                try {
                    try {
                        jedis.quit();
                    } catch (Exception e) {

                    }
                    jedis.disconnect();
                } catch (Exception e) {

                }
            }
        }
    }

    @Override
    public boolean validateObject(PooledObject<ShardedJedis> pooledShardedJedis) {
        try {
            ShardedJedis jedis = pooledShardedJedis.getObject();
            for (Jedis shard : jedis.getAllShards()) {
                if (!shard.ping().equals("PONG")) {
                    return false;
                }
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    @Override
    public void activateObject(PooledObject<ShardedJedis> p) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<ShardedJedis> p) throws Exception {

    }
}
