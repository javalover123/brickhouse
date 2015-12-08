package brickhouse.udf.xunit;

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

public abstract class XUnitUDTF extends GenericUDTF {
    abstract protected void incrCounter( String counterName, long counter);
	abstract protected void forwardXUnit( String xunit) throws HiveException;
}
