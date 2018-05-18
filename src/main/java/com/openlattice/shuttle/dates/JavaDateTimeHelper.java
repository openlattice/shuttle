package com.openlattice.shuttle.dates;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
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
                // transform to simpledateformat (to deal with eg. 95 --> should be 1995 not 2095)
                SimpleDateFormat frm = new SimpleDateFormat( datePatterns[i] );
                SimpleDateFormat to = new SimpleDateFormat("dd/MM/yyyy");
                // transform to YYYY-string
                String helpstring = to.format(frm.parse(date));
                DateTimeFormatter frmtr = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                return LocalDate.parse( helpstring, frmtr );
            } catch ( Exception e ) {
                if (i == datePatterns.length - 1){
                  logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
                }
            }
        }
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
                if (i == datePatterns.length - 1){
                  logger.error( "Unable to parse datetime {}, please see debug log for additional information.", date );
                }
            }
        }
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
                if (i == datePatterns.length - 1){
                  logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
                }
            }
        }
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
                if (i == datePatterns.length - 1){
                  logger.error( "Unable to parse tim {}, please see debug log for additional information.", time );
                }
            }
        }
        return null;
    }

}
