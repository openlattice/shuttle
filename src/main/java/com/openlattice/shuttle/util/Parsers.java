package com.openlattice.shuttle.util;

import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class Parsers {
    private static final Logger logger = LoggerFactory.getLogger( Parsers.class );

    public static String getAsString( Object obj ) {
        if ( obj != null && obj.toString() != null ) {
            return obj.toString().trim();
        }
        return null;
    }

    public static String parseDate( Object obj, String datePattern ) {
        String dateStr = getAsString( obj );
        if ( dateStr != null ) {
            DateTimeFormatter customDateFormatter = DateTimeFormat.forPattern( datePattern );
            LocalDate date = LocalDate.parse( dateStr, customDateFormatter );
            if ( date != null )
                return date.toString();
        }
        logger.error( "Unable to parse date \"{}\" for pattern \"{}\".", dateStr, datePattern );
        return null;
    }

    public static Integer parseInt( Object obj ) {
        String intStr = getAsString( obj );
        if ( intStr != null ) {
            try {
                int intValue = Integer.parseInt( intStr );
                return intValue;
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse int from value {}", intStr );
            }
        }
        return null;
    }

    public static Short parseShort( Object obj ) {
        String shortStr = getAsString( obj );
        if ( shortStr != null ) {
            try {
                short intValue = Short.parseShort( shortStr );
                return intValue;
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse short from value {}", shortStr );
            }
        }
        return null;
    }

    public static Long parseLong( Object obj ) {
        String longStr = getAsString( obj );
        if ( longStr != null ) {
            try {
                long longValue = Long.parseLong( longStr );
                return longValue;
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse long from value {}", longStr );
            }
        }
        return null;
    }

    public static Double parseDouble( Object obj ) {
        String doubleStr = getAsString( obj );
        if ( doubleStr != null ) {
            try {
                double doubleValue = Double.parseDouble( doubleStr );
                return doubleValue;
            } catch ( NumberFormatException e ) {
                logger.error( "Unable to parse double from value {}", doubleStr );
            }
        }
        return null;
    }

    public static UUID parseUUID( Object obj ) {
        String uuidStr = getAsString( obj );
        if ( uuidStr != null ) {
            try {
                return UUID.fromString( uuidStr );
            } catch ( IllegalArgumentException e ) {
                logger.error( "Unable to parse UUID from value {}", uuidStr );
            }
        }
        return null;
    }

}
