/*
 * Copyright 2016 Andrew Selden.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.bunnysoft.presto.type;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;

import static io.bunnysoft.presto.ElasticsearchErrorCode.ELASTICSEARCH_DATE_PARSE_ERROR;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;

/**
 * Utility for resolving Elasticsearch date format strings into the appropriate SQL type.
 *
 * For a full list of Elasticsearch date formats see:
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
 */
public final class DateTypeUtil
{
    public static final String DEFAULT_FORMAT = "strict_date_optional_time||epoch_millis";

    public static Type determineDateType(final String format)
    {
        final String _format = (format == null || format.isEmpty()) ? DEFAULT_FORMAT : format.trim();

        switch (_format)
        {
            case "basic_date":           // yyyyMMdd
                return DATE;
            case "basic_date_time":     // yyyyMMdd'T'HHmmss.SSSZ.
                return TIMESTAMP_WITH_TIME_ZONE;
            case "epoch_millis":
                return TIMESTAMP;
            default:
                throw new PrestoException(ELASTICSEARCH_DATE_PARSE_ERROR, "Failed to parse date format: [" + _format + "]");
        }
    }
}

/*
 DateTimeFormatter formatter;


        } else if ("basicDateTimeNoMillis".equals(input) || "basic_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicDateTimeNoMillis();
        } else if ("basicOrdinalDate".equals(input) || "basic_ordinal_date".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDate();
        } else if ("basicOrdinalDateTime".equals(input) || "basic_ordinal_date_time".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTime();
        } else if ("basicOrdinalDateTimeNoMillis".equals(input) || "basic_ordinal_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicOrdinalDateTimeNoMillis();
        } else if ("basicTime".equals(input) || "basic_time".equals(input)) {
            formatter = ISODateTimeFormat.basicTime();
        } else if ("basicTimeNoMillis".equals(input) || "basic_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicTimeNoMillis();
        } else if ("basicTTime".equals(input) || "basic_t_Time".equals(input)) {
            formatter = ISODateTimeFormat.basicTTime();
        } else if ("basicTTimeNoMillis".equals(input) || "basic_t_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicTTimeNoMillis();
        } else if ("basicWeekDate".equals(input) || "basic_week_date".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDate();
        } else if ("basicWeekDateTime".equals(input) || "basic_week_date_time".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTime();
        } else if ("basicWeekDateTimeNoMillis".equals(input) || "basic_week_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if ("date".equals(input)) {
            formatter = ISODateTimeFormat.date();
        } else if ("dateHour".equals(input) || "date_hour".equals(input)) {
            formatter = ISODateTimeFormat.dateHour();
        } else if ("dateHourMinute".equals(input) || "date_hour_minute".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinute();
        } else if ("dateHourMinuteSecond".equals(input) || "date_hour_minute_second".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecond();
        } else if ("dateHourMinuteSecondFraction".equals(input) || "date_hour_minute_second_fraction".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if ("dateHourMinuteSecondMillis".equals(input) || "date_hour_minute_second_millis".equals(input)) {
            formatter = ISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if ("dateOptionalTime".equals(input) || "date_optional_time".equals(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new FormatDateTimeFormatter(input,
                    ISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC),
                    ISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC), locale);
        } else if ("dateTime".equals(input) || "date_time".equals(input)) {
            formatter = ISODateTimeFormat.dateTime();
        } else if ("dateTimeNoMillis".equals(input) || "date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.dateTimeNoMillis();
        } else if ("hour".equals(input)) {
            formatter = ISODateTimeFormat.hour();
        } else if ("hourMinute".equals(input) || "hour_minute".equals(input)) {
            formatter = ISODateTimeFormat.hourMinute();
        } else if ("hourMinuteSecond".equals(input) || "hour_minute_second".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecond();
        } else if ("hourMinuteSecondFraction".equals(input) || "hour_minute_second_fraction".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondFraction();
        } else if ("hourMinuteSecondMillis".equals(input) || "hour_minute_second_millis".equals(input)) {
            formatter = ISODateTimeFormat.hourMinuteSecondMillis();
        } else if ("ordinalDate".equals(input) || "ordinal_date".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDate();
        } else if ("ordinalDateTime".equals(input) || "ordinal_date_time".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTime();
        } else if ("ordinalDateTimeNoMillis".equals(input) || "ordinal_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if ("time".equals(input)) {
            formatter = ISODateTimeFormat.time();
        } else if ("timeNoMillis".equals(input) || "time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.timeNoMillis();
        } else if ("tTime".equals(input) || "t_time".equals(input)) {
            formatter = ISODateTimeFormat.tTime();
        } else if ("tTimeNoMillis".equals(input) || "t_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.tTimeNoMillis();
        } else if ("weekDate".equals(input) || "week_date".equals(input)) {
            formatter = ISODateTimeFormat.weekDate();
        } else if ("weekDateTime".equals(input) || "week_date_time".equals(input)) {
            formatter = ISODateTimeFormat.weekDateTime();
        } else if ("weekDateTimeNoMillis".equals(input) || "week_date_time_no_millis".equals(input)) {
            formatter = ISODateTimeFormat.weekDateTimeNoMillis();
        } else if ("weekyear".equals(input) || "week_year".equals(input)) {
            formatter = ISODateTimeFormat.weekyear();
        } else if ("weekyearWeek".equals(input) || "weekyear_week".equals(input)) {
            formatter = ISODateTimeFormat.weekyearWeek();
        } else if ("weekyearWeekDay".equals(input) || "weekyear_week_day".equals(input)) {
            formatter = ISODateTimeFormat.weekyearWeekDay();
        } else if ("year".equals(input)) {
            formatter = ISODateTimeFormat.year();
        } else if ("yearMonth".equals(input) || "year_month".equals(input)) {
            formatter = ISODateTimeFormat.yearMonth();
        } else if ("yearMonthDay".equals(input) || "year_month_day".equals(input)) {
            formatter = ISODateTimeFormat.yearMonthDay();
        } else if ("epoch_second".equals(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(false), new EpochTimeParser(false)).toFormatter();
        } else if ("epoch_millis".equals(input)) {
            formatter = new DateTimeFormatterBuilder().append(new EpochTimePrinter(true), new EpochTimeParser(true)).toFormatter();
        // strict date formats here, must be at least 4 digits for year and two for months and two for day
        } else if ("strictBasicWeekDate".equals(input) || "strict_basic_week_date".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDate();
        } else if ("strictBasicWeekDateTime".equals(input) || "strict_basic_week_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTime();
        } else if ("strictBasicWeekDateTimeNoMillis".equals(input) || "strict_basic_week_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.basicWeekDateTimeNoMillis();
        } else if ("strictDate".equals(input) || "strict_date".equals(input)) {
            formatter = StrictISODateTimeFormat.date();
        } else if ("strictDateHour".equals(input) || "strict_date_hour".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHour();
        } else if ("strictDateHourMinute".equals(input) || "strict_date_hour_minute".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinute();
        } else if ("strictDateHourMinuteSecond".equals(input) || "strict_date_hour_minute_second".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecond();
        } else if ("strictDateHourMinuteSecondFraction".equals(input) || "strict_date_hour_minute_second_fraction".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondFraction();
        } else if ("strictDateHourMinuteSecondMillis".equals(input) || "strict_date_hour_minute_second_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.dateHourMinuteSecondMillis();
        } else if ("strictDateOptionalTime".equals(input) || "strict_date_optional_time".equals(input)) {
            // in this case, we have a separate parser and printer since the dataOptionalTimeParser can't print
            // this sucks we should use the root local by default and not be dependent on the node
            return new FormatDateTimeFormatter(input,
                    StrictISODateTimeFormat.dateOptionalTimeParser().withZone(DateTimeZone.UTC),
                    StrictISODateTimeFormat.dateTime().withZone(DateTimeZone.UTC), locale);
        } else if ("strictDateTime".equals(input) || "strict_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.dateTime();
        } else if ("strictDateTimeNoMillis".equals(input) || "strict_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.dateTimeNoMillis();
        } else if ("strictHour".equals(input) || "strict_hour".equals(input)) {
            formatter = StrictISODateTimeFormat.hour();
        } else if ("strictHourMinute".equals(input) || "strict_hour_minute".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinute();
        } else if ("strictHourMinuteSecond".equals(input) || "strict_hour_minute_second".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecond();
        } else if ("strictHourMinuteSecondFraction".equals(input) || "strict_hour_minute_second_fraction".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondFraction();
        } else if ("strictHourMinuteSecondMillis".equals(input) || "strict_hour_minute_second_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.hourMinuteSecondMillis();
        } else if ("strictOrdinalDate".equals(input) || "strict_ordinal_date".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDate();
        } else if ("strictOrdinalDateTime".equals(input) || "strict_ordinal_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTime();
        } else if ("strictOrdinalDateTimeNoMillis".equals(input) || "strict_ordinal_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.ordinalDateTimeNoMillis();
        } else if ("strictTime".equals(input) || "strict_time".equals(input)) {
            formatter = StrictISODateTimeFormat.time();
        } else if ("strictTimeNoMillis".equals(input) || "strict_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.timeNoMillis();
        } else if ("strictTTime".equals(input) || "strict_t_time".equals(input)) {
            formatter = StrictISODateTimeFormat.tTime();
        } else if ("strictTTimeNoMillis".equals(input) || "strict_t_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.tTimeNoMillis();
        } else if ("strictWeekDate".equals(input) || "strict_week_date".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDate();
        } else if ("strictWeekDateTime".equals(input) || "strict_week_date_time".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDateTime();
        } else if ("strictWeekDateTimeNoMillis".equals(input) || "strict_week_date_time_no_millis".equals(input)) {
            formatter = StrictISODateTimeFormat.weekDateTimeNoMillis();
        } else if ("strictWeekyear".equals(input) || "strict_weekyear".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyear();
        } else if ("strictWeekyearWeek".equals(input) || "strict_weekyear_week".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeek();
        } else if ("strictWeekyearWeekDay".equals(input) || "strict_weekyear_week_day".equals(input)) {
            formatter = StrictISODateTimeFormat.weekyearWeekDay();
        } else if ("strictYear".equals(input) || "strict_year".equals(input)) {
            formatter = StrictISODateTimeFormat.year();
        } else if ("strictYearMonth".equals(input) || "strict_year_month".equals(input)) {
            formatter = StrictISODateTimeFormat.yearMonth();
        } else if ("strictYearMonthDay".equals(input) || "strict_year_month_day".equals(input)) {
            formatter = StrictISODateTimeFormat.yearMonthDay();
        } else if (Strings.hasLength(input) && input.contains("||")) {
                String[] formats = Strings.delimitedListToStringArray(input, "||");
                DateTimeParser[] parsers = new DateTimeParser[formats.length];

                if (formats.length == 1) {
                    formatter = forPattern(input, locale).parser();
                } else {
                    DateTimeFormatter dateTimeFormatter = null;
                    for (int i = 0; i < formats.length; i++) {
                        FormatDateTimeFormatter currentFormatter = forPattern(formats[i], locale);
                        DateTimeFormatter currentParser = currentFormatter.parser();
                        if (dateTimeFormatter == null) {
                            dateTimeFormatter = currentFormatter.printer();
                        }
                        parsers[i] = currentParser.getParser();
                    }

                    DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(dateTimeFormatter.withZone(DateTimeZone.UTC).getPrinter(), parsers);
                    formatter = builder.toFormatter();
                }
        } else {
            try {
                formatter = DateTimeFormat.forPattern(input);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid format: [" + input + "]: " + e.getMessage(), e);
            }
        }
 */