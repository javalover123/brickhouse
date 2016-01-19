package tagged.udf.macros;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.format.DateTimeFormatter;

/**
 * Created by janandaram on 10/23/15.
 */
public class SpamDetector extends UDF {

    private Long getMin(Long x, Long y) {
        if(x==null && y==null) return null;
        else if(x==null && y != null) return y;
        else if(x !=null && y == null) return x;
        else return (x < y ? x : y);
    }

    private Long toYYYYMMDD(Long ts, DateTimeFormatter formatter) {
        return Long.parseLong(formatter.print(ts));
    }

    public Boolean evaluate(String interested_date , Long date_cancelled ,
                            Long date_boxed , Long date_spammer_added , Long date_spammer_removed) {
        // Choose the right date formatter depending on the length of interested_date
        DateTimeFormatter dtFormatter = null;
        if(interested_date.length()>8)
            dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMddHH");
        else
            dtFormatter = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

        Long interestedDate = Long.parseLong(interested_date);
        Long minDate = getMin(getMin(date_cancelled,date_boxed),date_spammer_added);
        if(minDate == null)
            minDate = 0L;
        else
            minDate = toYYYYMMDD(minDate, dtFormatter);


        Long minDate2 =  getMin(date_cancelled,date_boxed);
        if(minDate2==null)
            minDate2=0L;
        else
            minDate2 = toYYYYMMDD(minDate2, dtFormatter);

        // By default lets make everyone a spammer until proven otherwise
        boolean isSpammer=true;

        if( (date_cancelled==null && date_boxed == null && date_spammer_added == null) ||
                (date_spammer_removed == null && interestedDate < minDate) ||
                (date_spammer_removed != null &&
                        (
                                (date_spammer_added  > date_spammer_removed &&
                                        interestedDate < minDate && interestedDate > toYYYYMMDD(date_spammer_removed, dtFormatter))
                                        || (date_spammer_added  < date_spammer_removed &&
                                        (interestedDate < minDate ||
                                                (interestedDate > toYYYYMMDD(date_spammer_removed, dtFormatter) &&
                                                        (interestedDate < minDate2 || (date_cancelled == null && date_boxed == null)
                                                        )
                                                )
                                        )
                                )
                        )
                ))
            isSpammer = false;

        return isSpammer;
    }
}