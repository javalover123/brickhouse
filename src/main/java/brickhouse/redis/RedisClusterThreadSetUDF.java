package brickhouse.redis;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.IntWritable;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.exceptions.JedisException;

import java.util.HashMap;
import java.util.Map;

/*
--加大map数量，提高并发率:
set mapred.max.split.size=4194304;
set mapred.min.split.size.per.node=4194304;
set mapred.min.split.size.per.rack=4194304;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
set mapreduce.job.queuename=root.etl;
select
 xydb.redis_cluster_thread_set('192.168.110.87:6379',concat('xydbuid.',uid), value, redis_password) as result
  FROM xydb.yydb_user_act_xydb WHERE ds='2017-08-10' AND type='buy_lottery'
 */
@Description(name = "redis_cluster_set",
        value = "_FUNC_(host_and_port,redis_key, redis_value, redis_password) - Return ret "
)
public class RedisClusterThreadSetUDF extends GenericUDF {

    private HostAndPort hostAndPort;
    private StringObjectInspector paramsElementInspector;
    private StringObjectInspector keyFieldOI;
    private StringObjectInspector passwordFieldOI;


    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        //JedisCluster只能在方法内部创建作为局部变量使用，这个类里面会用到连接池，连接池是有状态的，无法序列化。
        try {
            long start = System.currentTimeMillis();
            String data = paramsElementInspector.getPrimitiveJavaObject(arg0[2].get());
            String did = keyFieldOI.getPrimitiveJavaObject(arg0[1].get());
            String password = keyFieldOI.getPrimitiveJavaObject(arg0[3].get());

            try {
                System.out.println("submit:" + did + ",data:" + data);
                //改成hmset将多个属性一次提交过去
                JedisClusterUtil.getJedisCluster(hostAndPort, password).set(did, data);
            } catch (JedisException e) {
                e.printStackTrace();
                System.out.println("retry submit:" + did + ",data:" + data);
                try {
                    JedisClusterUtil.getJedisCluster(hostAndPort, password).set(did, data);
                } catch (JedisException ex) {
                    ex.printStackTrace();
                    throw new HiveException(ex);
                }
            }

            System.out.println("cost :" + (System.currentTimeMillis() - start));
            return new IntWritable(1);
        } catch (Exception e) {
            e.printStackTrace();
            throw new HiveException(e);
        }
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "redis_cluster_thread_set(host_and_port,redis_key, redis_value, redis_password)";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 4) {
            throw new UDFArgumentException(" Expecting   two  arguments:<redishost:port>  <redis_key> <redis_value> <redis_password>");
        }
        //第一个参数校验
        if (arg0[0].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("redis host:port  must be constant");
            }
            ConstantObjectInspector redishost_and_port = (ConstantObjectInspector) arg0[0];

            String[] host_and_port = redishost_and_port.getWritableConstantValue().toString().split(":");
            hostAndPort = new HostAndPort(host_and_port[0], Integer.parseInt(host_and_port[1]));

        }

        //第2个参数校验
        if (arg0[1].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[1] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("redis set key   must be string");
            }
            keyFieldOI = (StringObjectInspector) arg0[1];
        }

        //第3个参数校验
        if (arg0[2].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[2]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[2] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("redis set value   must be string");
            }
        }
//        MapObjectInspector third = (MapObjectInspector) arg0[2];
        paramsElementInspector = (StringObjectInspector) arg0[2];
//        System.out.println(paramsElementInspector.getMapKeyObjectInspector().getCategory());
//        System.out.println(paramsElementInspector.getMapValueObjectInspector().getCategory());

        //第4个参数校验
        if (arg0[3].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[3]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[3] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("redis password   must be string");
            }
            keyFieldOI = (StringObjectInspector) arg0[3];
        }

        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

}
