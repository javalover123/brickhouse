package brickhouse.redis;

/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/
import brickhouse.hbase.HTableFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.util.JedisURIHelper;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

/**
 * Retrieve from Redis by doing bulk s from an aggregate function call.
 */

@Description(name = "redis_batch_set",
        value = "_FUNC_(config_map, key, value) - Perform batch Redis updates of a table "
)
public class RedisClusterBatchSetUDAF extends AbstractGenericUDAFResolver {
    private static final Logger LOG = Logger.getLogger(RedisClusterBatchSetUDAF.class);


    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters)
            throws SemanticException {
        for (int i = 0; i < parameters.length; ++i) {
            LOG.info(" BATCH SET PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
            System.out.println(" BATCH SET PARAMETERS : " + i + " -- " + parameters[i].getTypeName() + " cat = " + parameters[i].getCategory());
        }

        return new BatchPutUDAFEvaluator();
    }

    public static class BatchPutUDAFEvaluator extends GenericUDAFEvaluator {
        private Reporter reporter;

        private Reporter getReporter() throws HiveException {
            try {
                if (reporter == null) {
                    Class clazz = Class.forName("org.apache.hadoop.hive.ql.exec.MapredContext");
                    Method staticGetMethod = clazz.getMethod("get");
                    Object mapredObj = staticGetMethod.invoke(null);
                    Class mapredClazz = mapredObj.getClass();
                    Method getReporter = mapredClazz.getMethod("getReporter");
                    Object reporterObj = getReporter.invoke(mapredObj);

                    reporter = (Reporter) reporterObj;
                }
                return reporter;
            } catch (Exception e) {
                throw new HiveException("Error while accessing Hadoop Counters", e);
            }
        }

        public class PutBuffer implements AggregationBuffer {
            public Map<String, String> setList;

            public PutBuffer() {
            }

            public void reset() {
                setList = new LinkedHashMap<>(13000);
            }

            public void addKeyValue(String key, String val) throws HiveException {
                setList.put(key, val);
            }
        }

        private int batchSize = 10000;
        private int numPutRecords = 0;

        public static final String BATCH_SIZE_TAG = "batch_size";

        // For PARTIAL1 and COMPLETE: ObjectInspectors for original data
		private PrimitiveObjectInspector inputKeyOI;
		private PrimitiveObjectInspector inputValOI;
        // For PARTIAL2 and FINAL: ObjectInspectors for partial aggregations (list
        // of objs)
        private StandardListObjectInspector listKVOI;
        private Map<String, String> configMap;

        private JedisCluster table;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
			// init output object inspectors
			///  input will be key, value and batch size
            LOG.info(" Init mode = " + m);
            System.out.println(" Init mode = " + m);
            System.out.println(" parameters =  = " + parameters + " Length = " + parameters.length);
            configMap = new HashMap<String, String>();
            for (int k = 0; k < parameters.length; ++k) {
                LOG.info("Param " + k + " is " + parameters[k]);
                System.out.println("Param " + k + " is " + parameters[k]);
            }

            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
                configMap = HTableFactory.getConfigFromConstMapInspector(parameters[0]);

				inputKeyOI = (PrimitiveObjectInspector) parameters[1];
				inputValOI = (PrimitiveObjectInspector) parameters[2];

                try {
                    LOG.info(" Initializing HTable ");
                    String host = configMap.get("uri");
                    URI uri = URI.create(host);
                    HostAndPort hostAndPort = new HostAndPort(uri.getHost(), uri.getPort());
                    String password = JedisURIHelper.getPassword(uri);
                    table = JedisClusterUtil.getJedisCluster(hostAndPort, password);

                    if (configMap.containsKey(BATCH_SIZE_TAG)) {
                        batchSize = Integer.parseInt(configMap.get(BATCH_SIZE_TAG));
                    }
                } catch (Exception e) {
                    throw new HiveException(e);
                }
            } else {
                listKVOI = (StandardListObjectInspector) parameters[0];

            }

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                return ObjectInspectorFactory
                        .getStandardListObjectInspector(
                                ObjectInspectorFactory.getStandardListObjectInspector(
                                        PrimitiveObjectInspectorFactory.javaStringObjectInspector));
            } else {
                /// Otherwise return a message
                return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            PutBuffer buff = new PutBuffer();
            reset(buff);
            return buff;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            String key = getByteString(parameters[1], inputKeyOI);
            String val = getByteString(parameters[2], inputValOI);

            if (key != null && val != null) {
            PutBuffer kvBuff = (PutBuffer) agg;
            kvBuff.addKeyValue(key, val);

            if (kvBuff.setList.size() >= batchSize) {
                batchUpdate(kvBuff, false);
            }
            } else {
                getReporter().getCounter(BatchPutUDAFCounter.NULL_KEY_OR_VALUE_INSERT_FAILURE).increment(1);
            }
        }


        /**
         * @param obj
         * @param objInsp
         * @return
         */
        private String getByteString(Object obj, PrimitiveObjectInspector objInsp) {
            switch (objInsp.getPrimitiveCategory()) {
                case STRING:
                    StringObjectInspector strInspector = (StringObjectInspector) objInsp;
                    return strInspector.getPrimitiveJavaObject(obj);
                case BINARY:
                    BinaryObjectInspector binInspector = (BinaryObjectInspector) objInsp;
                    return new String(binInspector.getPrimitiveJavaObject(obj));
                /// XXX TODO interpret other types, like ints or doubled
                default:
                    return null;
            }
        }

        protected void batchUpdate(PutBuffer kvBuff, boolean flushCommits) throws HiveException {
            try (JedisClusterPipeline pipeline = JedisClusterPipeline.pipelined(table)) {
                for (Map.Entry<String, String> entry : kvBuff.setList.entrySet()) {
                    pipeline.set(entry.getKey(), entry.getValue());
                }
                pipeline.sync();
                numPutRecords += kvBuff.setList.size();
                if (kvBuff.setList.size() > 0)
                    LOG.info(" Doing Batch Set " + kvBuff.setList.size() + " records; Total set records = " + numPutRecords + " ; Start = " + kvBuff.setList.entrySet().iterator().next());
                else
                    LOG.info(" Doing Batch Put with ZERO 0 records");

                getReporter().getCounter(BatchPutUDAFCounter.NUMBER_OF_SUCCESSFUL_PUTS).increment(kvBuff.setList.size());
                getReporter().getCounter(BatchPutUDAFCounter.NUMBER_OF_BATCH_OPERATIONS).increment(1);
                kvBuff.setList.clear();
            } catch (Exception e) {
                throw new HiveException(e);
            }
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            PutBuffer myagg = (PutBuffer) agg;
            List<Object> partialResult = (List<Object>) this.listKVOI.getList(partial);
            ListObjectInspector subListOI = (ListObjectInspector) listKVOI.getListElementObjectInspector();

            List first = subListOI.getList(partialResult.get(0));
            //// Include arbitrary configurations, by adding strings of the form k=v
            for (int j = 0; j < first.size(); ++j) {
                String kvStr = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(first.get(j));
                String[] kvArr = kvStr.split("=");
                if (kvArr.length == 2) {
                    configMap.put(kvArr[0], kvArr[1]);
                }
            }

            for (int i = 1; i < partialResult.size(); ++i) {

                List kvList = subListOI.getList(partialResult.get(i));
                String key = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(0));
                String val = ((StringObjectInspector) (subListOI.getListElementObjectInspector())).getPrimitiveJavaObject(kvList.get(1));

                myagg.addKeyValue(key, val);

            }

            if (myagg.setList.size() >= batchSize) {
                batchUpdate(myagg, false);
            }
        }

        @Override
        public void reset(AggregationBuffer buff) throws HiveException {
            PutBuffer setBuffer = (PutBuffer) buff;
            setBuffer.reset();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            PutBuffer myagg = (PutBuffer) agg;
            batchUpdate(myagg, true);
            return "Finished Batch updates ; Num Puts = " + numPutRecords;

        }


        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            PutBuffer myagg = (PutBuffer) agg;

            ArrayList<List<String>> ret = new ArrayList<List<String>>();

            return ret;
        }
    }

    private static enum BatchPutUDAFCounter {
        NULL_KEY_OR_VALUE_INSERT_FAILURE, NUMBER_OF_SUCCESSFUL_PUTS, NUMBER_OF_BATCH_OPERATIONS;
    }

}
