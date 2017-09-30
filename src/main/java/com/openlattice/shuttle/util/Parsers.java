package com.openlattice.shuttle.util;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parsers {
    private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    public static String parseDate( String rawDate, String datePattern ) {
        if ( rawDate != null ) {
            String d = rawDate.toString();
            DateTimeFormatter customDateFormatter = DateTimeFormat.forPattern( datePattern );
            LocalDate date = LocalDate.parse( rawDate, customDateFormatter );
            if ( date != null )
                return date.toString();
        }
        logger.error( "Unable to parse date \"{}\" for pattern \"{}\".", rawDate, datePattern );
        return null;
    }

    public static Integer parseInt( String rawInt ) {
        try {
            int intValue = Integer.parseInt( rawInt );
            return intValue;
        } catch ( NumberFormatException e ) {
            logger.error( "Unable to parse int from value {}", rawInt );
            return null;
        }
    }

    public static Short parseShort( String rawShort ) {
        try {
            short intValue = Short.parseShort( rawShort );
            return intValue;
        } catch ( NumberFormatException e ) {
            logger.error( "Unable to parse short from value {}", rawShort );
            return null;
        }
    }

    public static Long parseLong( String rawLong ) {
        try {
            long longValue = Long.parseLong( rawLong );
            return longValue;
        } catch ( NumberFormatException e ) {
            logger.error( "Unable to parse long from value {}", rawLong );
            return null;
        }
    }

    public static Double parseDouble( String rawDouble ) {
        try {
            double longValue = Double.parseDouble( rawDouble );
            return longValue;
        } catch ( NumberFormatException e ) {
            logger.error( "Unable to parse double from value {}", rawDouble );
            return null;
        }
    }

}
