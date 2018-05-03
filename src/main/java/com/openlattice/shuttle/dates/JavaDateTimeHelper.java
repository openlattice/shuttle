package com.openlattice.shuttle.dates;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;

public class JavaDateTimeHelper {
    private static final Logger logger = LoggerFactory.getLogger( JavaDateTimeHelper.class );

    private final     TimeZone                tz;
    private final     String[]                datePatterns;
    private transient List<DateTimeFormatter> formatters;

    public JavaDateTimeHelper( TimeZone tz, String... datePatterns ) {
        this.tz = tz;
        this.datePatterns = datePatterns;
        this.formatters = Arrays.stream( datePatterns ).map( pattern -> DateTimeFormatter.ofPattern( pattern ) )
                .collect( Collectors.toList() );
    }

    public String parseDate( String date ) {
        if ( StringUtils.isBlank( date ) )
            return null;
        LocalDate ld = parseLocalDate( date );
        return ld == null ? null : ld.toString();
    }

    private LocalDate parseLocalDate( String date ) {
        if ( date == null || date.equals( "NULL" ) ) {
            return null;
        }

        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            if ( formatter == null ) {
                formatter = DateTimeFormatter.ofPattern( datePatterns[ i ] );
                formatters.set( i, formatter );
            }
            try {
                return LocalDate.parse( date, formatter );
            } catch ( Exception e ) {
                logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
            }
        }
        logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
        return null;
    }

    public String parseDateTime( String date ) {
        if ( StringUtils.isBlank( date ) )
            return null;
        OffsetDateTime odt = parseOffsetDateTime( date );
        return odt == null ? null : odt.toString();
    }

    private OffsetDateTime parseOffsetDateTime( String date ) {
        if ( date == null || date.equals( "NULL" ) ) {
            return null;
        }

        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            if ( formatter == null ) {
                formatter = DateTimeFormatter.ofPattern( datePatterns[ i ] );
                formatters.set( i, formatter );
            }

            try {
                LocalDateTime ldt = LocalDateTime.parse( date, formatter );

                return ldt.atZone( tz.toZoneId() ).toOffsetDateTime();
            } catch ( Exception e ) {
                logger.debug( "Unable to parse datetime {} with format string {}", date, datePatterns[ i ], e );
            }
        }
        logger.error( "Unable to parse datetime {}, please see debug log for additional information.", date );
        return null;
    }

    public String parseDateAsDateTime( String date ) {
        if ( StringUtils.isBlank( date ) )
            return null;
        OffsetDateTime odt = parseDateAsOffsetDateTime( date );
        return odt == null ? null : odt.toString();
    }

    private OffsetDateTime parseDateAsOffsetDateTime( String date ) {
        if ( date == null || date.equals( "NULL" ) ) {
            return null;
        }

        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            if ( formatter == null ) {
                formatter = DateTimeFormatter.ofPattern( datePatterns[ i ] );
                formatters.set( i, formatter );
            }

            try {
                LocalDateTime ldt = LocalDate.parse( date, formatter ).atTime( 0, 0 );

                return ldt.atZone( tz.toZoneId() ).toOffsetDateTime();
            } catch ( Exception e ) {
                logger.debug( "Unable to parse datetime {} with format string {}", date, datePatterns[ i ], e );
            }
        }
        logger.error( "Unable to parse datetime {}, please see debug log for additional information.", date );
        return null;
    }

    public String parseTime( String time ) {
        if ( StringUtils.isBlank( time ) )
            return null;
        LocalTime lt = parseLocalTime( time );
        return lt == null ? null : lt.toString();
    }

    private LocalTime parseLocalTime( String time ) {
        if ( StringUtils.isBlank( time ) || time.equals( "NULL" ) )
            return null;

        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            if ( formatter == null ) {
                formatter = DateTimeFormatter.ofPattern( datePatterns[ i ] );
                formatters.set( i, formatter );
            }

            try {
                return LocalTime.parse( time, formatter );
            } catch ( Exception e ) {
                logger.debug( "Unable to parse time {} with format string {}", time, datePatterns[ i ], e );
            }
        }
        logger.error( "Unable to parse time {}, please see debug log for additional information.", time );
        return null;
    }

}
