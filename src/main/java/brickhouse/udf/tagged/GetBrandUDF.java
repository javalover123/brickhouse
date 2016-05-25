package brickhouse.udf.tagged;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

public class GetBrandUDF extends UDF {

    private static final String BRAND_TAGGED = "tagged";
    private static final String BRAND_HI5 = "hi5";
    private static final String BRAND_ONE = "one";

    @Description(name="getBrand",
            value = "_FUNC_(a) - Figures out the brand based on the type value"
    )
    public String evaluate(Integer type) {
        if (type == null) {
            return BRAND_TAGGED;
        } else {
            if (type == -1)
                return BRAND_HI5;
            else if (type == 5)
                return BRAND_ONE;
            else
                return BRAND_TAGGED;
        }
    }
}
