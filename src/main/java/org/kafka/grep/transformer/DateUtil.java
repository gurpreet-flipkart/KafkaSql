package org.kafka.grep.transformer;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.TimeZone;


public class DateUtil {

  private static final DateTimeZone timeZone = DateTimeZone.forTimeZone(TimeZone.getTimeZone("IST"));

  public static String getDateIfParseable(String literal) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(timeZone);
    Long time = getTime(literal, dateTimeFormatter);
    if (time == null) {
      dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ssZ");
      time = getTime(literal, dateTimeFormatter);
      if (time != null) {
        return time.toString();
      }
      return literal;
    } else {
      return time.toString();
    }
  }

  private static Long getTime(String literal, DateTimeFormatter possibleDateFormat) {
    try {
      return possibleDateFormat.parseDateTime(literal).toDate().getTime();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }
}
