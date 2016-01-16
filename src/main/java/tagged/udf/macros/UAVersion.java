package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by janandaram on 10/27/15.
 */
public class UAVersion extends UDF {

    /*
    create temporary macro ua_version(user_agent string)

case when ua_platform(user_agent) in ('Android','Prime','iOS') then
    case when user_agent like '%,%' and user_agent like '%6.6.%' then
        split(split(user_agent,',')[0],' ')[1]

    else when split(user_agent,'/')[1] like '% %' then
            case when regexp_replace(split(split(user_agent,'/')[1],' ')[0],'[a-zA-Z]','') = split(split(user_agent,'/')[1],' ')[0] then
                case when split(split(user_agent,'/')[1],' ')[0] = '10'
                    then '7.2.0'
                    else split(split(user_agent,'/')[1],' ')[0]
                end
            else
                null
            end
        when regexp_replace(split(user_agent,'/')[1],'[a-zA-Z]','') = split(user_agent,'/')[1]
            then split(user_agent,'/')[1]
            else null
        end
    else
        null
end;
     */
    public String evaluate(String userAgent) {
        String platform = UAPlatform.getUAPlatform(userAgent);

        if(platform.matches("Android|Prime|iOS")) {
            if(userAgent.contains(",") && userAgent.contains("6.6."))
                return userAgent.split(",")[0].split(" ")[1];
            else if(userAgent.split("/")[1].contains(" "))
                if(userAgent.split("/")[1].split(" ")[0].replace("[a-zA-Z]", "").equals(userAgent.split("/")[1].split(" ")[0]))
                    if(userAgent.split("/")[1].split(" ")[0].equals("10"))
                        return "7.2.0";
                    else
                        return userAgent.split("/")[1].split(" ")[0];
            else if(userAgent.split("/")[1].replace("[a-zA-Z]", "").equals(userAgent.split("/")[1]))
                return userAgent.split("/")[1];
        }

        return null;
    }
}
