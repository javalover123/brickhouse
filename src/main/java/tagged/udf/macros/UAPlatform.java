package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/27/15.
 *
 * Please do not modify this UDF as it returns exactly the same results as the HIVE ua_platform UDF as shown below:
 * modified by Marcin 01/15/2016
 *
 create temporary macro ua_platform(user_agent string)
 case when ((lower(user_agent) like 'tagged%' or lower(user_agent) like 'hi5%') and (lower(user_agent) like '%/an%' or lower(user_agent) like '%android%')) or lower(user_agent) like 'dalvik%' then 'Android'
 when (lower(user_agent) like 'tagged%' or lower(user_agent) like 'hi5%') and lower(user_agent) like '%darwin%' then 'iOS'
 when lower(user_agent) not like 'tagged%' and lower(user_agent) not like 'hi5%' and
 (lower(user_agent) like '%mobi%'
 or lower(user_agent) like '%android%'
 or lower(user_agent) like '%blackberry%'
 or lower(user_agent) like '%nokia%'
 or lower(user_agent) like '%opera%j2me%'
 or lower(user_agent) like '%opera%midp%'
 or lower(user_agent) like '%opera%series+60%'
 or lower(user_agent) like '%samsung%'
 or lower(user_agent) like '%iphone%') then 'Mobile Web'
 when lower(user_agent) is not null and lower(user_agent) not like '' then 'Desktop Web'
 else 'Unknown' end;
 *
 */
public class UAPlatform extends UDF {
    private static final String TAGGED = "tagged";
    private static final String ANDROID = "android";
    private static final String HI5 = "hi5";

    public String evaluate(String userAgent) {
        return getUAPlatform(userAgent);
    }

    public static String getUAPlatform(String userAgent) {
        if(userAgent == null || userAgent.length() == 0) return "Unknown";

        String lowerUserAgent = userAgent.toLowerCase();
        if((lowerUserAgent.contains(TAGGED) || lowerUserAgent.contains(HI5)) &&
                (lowerUserAgent.contains("/an") || lowerUserAgent.contains(ANDROID)) ||
                lowerUserAgent.contains("dalvik"))
            return "Android";

        if((lowerUserAgent.contains(TAGGED) || lowerUserAgent.contains(HI5)) &&
                lowerUserAgent.contains("darwin"))
            return "iOS";


        if (!lowerUserAgent.contains(TAGGED) && !lowerUserAgent.contains(HI5) &&
                (lowerUserAgent.contains("mobi") || lowerUserAgent.contains(ANDROID) ||
                        lowerUserAgent.contains("blackberry") || lowerUserAgent.contains("nokia") ||
                        lowerUserAgent.matches(".*opera.*j2me.*")  ||
                        lowerUserAgent.matches(".*opera.*midp.*")  ||
                        lowerUserAgent.matches(".*opera.*series\\+60.*") ||
                        lowerUserAgent.contains("samsung") || lowerUserAgent.contains("iphone") )

           )
            return "Mobile Web";

        return "Desktop Web";
    }
}