package org.kafka.grep.utils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.TimeZone;

public class Utils {
    public static final DateTimeZone IST = DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"));

    public static String convertTime(String time) {
        DateTimeFormatter dtf = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(IST);
        DateTimeFormatter end = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(IST);
        DateTime dateTime = dtf.parseDateTime(time);
        return end.print(dateTime);
    }

    public static String getTime(Long time) {
        DateTimeFormatter end = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(IST);
        return end.print(time);
    }

    public static long getTime(String time) {
        return DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(IST).parseDateTime(time).getMillis();

    }

    public static long getForSimpleTime(String time) {
        return DateTimeFormat.forPattern("yyyy-MM-dd").withZone(IST).parseDateTime(time).getMillis();

    }


    public static long getForSimpleTimeStartOfDay(String time) {
        return DateTimeFormat.forPattern("yyyy-MM-dd").withZone(IST).parseDateTime(time).withTimeAtStartOfDay().getMillis();

    }

    public static String toSimpleTime(long time) {
        return DateTimeFormat.forPattern("yyyy-MM-dd").withZone(IST).print(time);

    }

    public static String toSimpleTimeAfterdays(String date, int days) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(IST);
        return dateTimeFormatter.print(dateTimeFormatter.parseDateTime(date).plusDays(days));

    }

    public static String toSimpleTimeBeforedays(String date, int days) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(IST);
        return dateTimeFormatter.print(dateTimeFormatter.parseDateTime(date).minusDays(days));

    }
}
