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

    public static Integer parseInt( Object obj ) {
        String intStr = getAsString( obj );
        if ( intStr != null ) {
            try {
                return Integer.parseInt( intStr );
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
                return Short.parseShort( shortStr );
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
                return Long.parseLong( longStr );
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
                return Double.parseDouble( doubleStr );
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