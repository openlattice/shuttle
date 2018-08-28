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
                LocalDate ld = LocalDate.parse( date, formatter );
                if (
                        datePatterns[ 0 ].matches( ".*yy.*" ) && !datePatterns[ 0 ].matches( ".*yyyy.*" ) ||
                                datePatterns[ 0 ].matches( ".*YY.*" ) && !datePatterns[ 0 ].matches( ".*YYYY.*" )
                        ) {
                    if ( ( ld.getYear() - LocalDate.now().getYear() ) > 20 ) {
                        ld = ld.withYear( ld.getYear() - 100 );
                    }
                }
                return ld;
            } catch ( Exception e ) {
                if ( i == datePatterns.length - 1 ) {
                    logger.error( "Unable to parse date {}, please see debug log for additional information.", date );
                }
            }
        }
        return null;
    }

    public LocalDate parseDateTimeAsDate( String date ) {
        if ( shouldIgnoreValue( date ) )
            return null;
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            try {
                LocalDateTime ldt = LocalDateTime.parse( date, formatter );

                return ldt.atZone( tz.toZoneId() ).toLocalDate();
            } catch ( Exception e ) {
                if ( i == datePatterns.length - 1 ) {
                    logger.error( "Unable to parse datetime {}, please see debug log for additional information.",
                            date );
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
                if ( i == datePatterns.length - 1 ) {
                    logger.error( "Unable to parse datetime {}, please see debug log for additional information.",
                            date );
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
                if ( i == datePatterns.length - 1 ) {
                    logger.error( "Unable to parse datetime {}, please see debug log for additional information.",
                            date );
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
                if ( i == datePatterns.length - 1 ) {
                    logger.error( "Unable to parse time {}, please see debug log for additional information.", time );
                }
            }
        }
        return null;
    }

}
