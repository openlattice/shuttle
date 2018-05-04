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

    private final TimeZone                tz;
    private final String[]                datePatterns;
    private final List<DateTimeFormatter> formatters;

    public JavaDateTimeHelper( TimeZone tz, String... datePatterns ) {
        this.tz = tz;
        this.datePatterns = datePatterns;
        this.formatters = Arrays.stream( datePatterns ).map( pattern -> DateTimeFormatter.ofPattern( pattern ) )
                .collect( Collectors.toList() );
    }

    private boolean shouldIgnoreValue( String date ) {
        return StringUtils.isBlank( date ) || date.equals( "NULL" );
    }

    public LocalDate parseDate( String date ) {
        if ( shouldIgnoreValue( date ) )
            return null;
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            try {
                return LocalDate.parse( date, formatter );
            } catch ( Exception e ) {
                logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
            }
        }
        logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
        return null;
    }

    public OffsetDateTime parseDateTime( String date ) {
        if ( shouldIgnoreValue( date ) )
            return null;
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
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

    public OffsetDateTime parseDateAsDateTime( String date ) {
        if ( shouldIgnoreValue( date ) )
            return null;
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            try {
                LocalDateTime ldt = LocalDate.parse( date, formatter ).atTime( 0, 0 );

                return ldt.atZone( tz.toZoneId() ).toOffsetDateTime();
            } catch ( Exception e ) {
                logger.debug( "Unable to parse date as datetime {} with format string {}", date, datePatterns[ i ], e );
            }
        }
        logger.error( "Unable to parse date as datetime {}, please see debug log for additional information.", date );
        return null;
    }

    public LocalTime parseTime( String time ) {
        if ( shouldIgnoreValue( time ) )
            return null;
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
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
