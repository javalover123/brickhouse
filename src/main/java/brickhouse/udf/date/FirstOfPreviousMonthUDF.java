package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;


/**
 * Given a date in YYYYMMDD format, this function figures out previous month first of the moth date. For example:
 *  select firstOfPrevMonth(20160101) would return 20151201
 *
 *@author Marcin Michalski
 */

@Description(name = "firstOfPrevMonth",
        value = "_FUNC_(YYYYMMDD) - Generates number of calander days (1-365) for a given date string ")
public class FirstOfPreviousMonthUDF extends UDF {
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYYMMdd");

    public String evaluate(String dateStr) {
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        DateTime firstOfMonthDt = dt.minusMonths(1).withDayOfMonth(1);
        return YYYYMMDD.print(firstOfMonthDt);
    }
}
