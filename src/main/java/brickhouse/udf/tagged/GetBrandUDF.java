package brickhouse.udf.tagged;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

public class GetBrandUDF extends UDF {

    private static final String BRAND_TAGGED = "tagged";
    private static final String BRNAD_HI5 = "hi5";

    @Description(name="getBrand",
            value = "_FUNC_(a) - Figures out the brand based on the type value"
    )
    public String evaluate(Integer type) {
        if(type == null || type != -1)
            return BRAND_TAGGED;
        else
            return BRNAD_HI5;
    }
}
