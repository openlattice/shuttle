package com.openlattice.shuttle.dates;

import com.openlattice.shuttle.util.Constants;
import com.openlattice.shuttle.util.Cached;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;

public class JavaDateTimeHelper {
    private static final Logger logger = LoggerFactory.getLogger( JavaDateTimeHelper.class );

    private       ZoneId   zoneId;
    private       boolean  shouldAddTimezone;
    private final String[] datePatterns;

    public JavaDateTimeHelper( Optional<TimeZone> tz, String... datePatterns ) {
        this.datePatterns = datePatterns;
        this.zoneId = tz.orElse( Constants.DEFAULT_TIMEZONE ).toZoneId();
        this.shouldAddTimezone = tz.isPresent();
    }

    /**
     * Parse datetime into OffsetDateTime:
     * - parse into OffsetDateTime as ISO + check whether timezones match
     * - parse into OffsetDateTime with provided patterns + perform 2-digit-year-fix + check whether timezones match
     * - parse into LocalDateTime as ISO + add timezone
     * - parse into LocalDateTime with provided patterns + perform 2-digit-year-fix + add timezone
     *
     * @param date - String to parse
     */
    public OffsetDateTime parseDateTime( String date ) {

        if ( StringUtils.isBlank( date ) || date.equals( "NULL" ) )
            return null;

        // Try parsing into a OffsetDateTime
        OffsetDateTime odt = parseFromOffsetDateTime( date );
        if ( shouldAddTimezone )
            TimeZones.checkTimezonesMatch( odt, zoneId );
        if ( odt != null )
            return odt;

        // Try parsing into OffsetDateTime with patterns
        OffsetDateTime odt_p = parseFromPatterns(
                date,
                ( toParse, formatter ) -> OffsetDateTime.parse( toParse, formatter ),
                ( local_odt, datePattern ) -> DecadeChangeHelper
                        .fixTwoYearPatternOffsetDateTime( local_odt, datePattern ) );
        if ( shouldAddTimezone )
            TimeZones.checkTimezonesMatch( odt, zoneId );
        if ( odt_p != null )
            return odt_p;

        // Try parsing into a LocalDateTime
        LocalDateTime ldt = parseFromLocalDateTime( date );
        if ( ldt != null )
            return ldt.atZone( zoneId ).toOffsetDateTime();

        // Try parsing into a LocalDateTime with patterns
        LocalDateTime ldt_p = parseFromPatterns(
                date,
                ( toParse, formatter ) -> LocalDateTime.parse( toParse, formatter ),
                ( local_odt, datePattern ) -> DecadeChangeHelper.fixTwoYearPatternLocalDate( local_odt, datePattern ) );
        if ( ldt_p != null )
            return ldt_p.atZone( zoneId ).toOffsetDateTime();

        logger.error( "Could not parse Date Time " + date );
        return null;
    }

    /**
     * Loops through datePatterns and tries to parse input
     *
     * @param date              - String to parse
     * @param parseFunction     - function to use for parsing (different depending on output requirement)
     * @param postParseFunction - function to apply after parsing, depending on successful pattern
     */
    public <R> R parseFromPatterns(
            String date,
            BiFunction<String, DateTimeFormatter, R> parseFunction,
            BiFunction<R, String, R> postParseFunction ) {
        for ( int i = 0; i < datePatterns.length; i++ ) {
            try {
                DateTimeFormatter formatter = Cached.getDateFormatForString( datePatterns[ i ] );
                R result = parseFunction.apply( date, formatter );
                return postParseFunction.apply( result, datePatterns[ i ] );
            } catch ( DateTimeParseException e ) {
                // do nothing
            } catch ( ExecutionException ex ) {
                logger.error( "ExecutionException loading pattern from cache", ex );
            }

        }
        return null;
    }

    /**
     * Parses a String into an OffsetDateTime
     *
     * @param date - String to parse
     */
    public OffsetDateTime parseFromOffsetDateTime( String date ) {
        try {
            OffsetDateTime odt = OffsetDateTime.parse( date );
            return odt;
        } catch ( DateTimeParseException eAutoParseODT ) {
            return null;
        }
    }

    /**
     * Parses a String into a LocalDateTime
     *
     * @param date - String to parse
     */
    public LocalDateTime parseFromLocalDateTime( String date ) {
        try {
            LocalDateTime ldt = Timestamp.valueOf( date ).toLocalDateTime();
            return ldt;
        } catch ( IllegalArgumentException | DateTimeParseException eAutoParseLDT ) {
            return null;
        }
    }

    public LocalDate parseDateTimeAsDate( String date ) {
        if ( StringUtils.isBlank( date ) || date.equals( "NULL" ) )
            return null;
        OffsetDateTime odt = parseDateTime( date );
        if ( odt == null )
            return null;
        LocalDateTime ldt = odt.toLocalDateTime();
        if ( ldt == null ) {
            return null;
        }
        return ldt.atZone( zoneId ).toLocalDate();
    }

    public LocalTime parseDateTimeAsTime( String datetime ) {
        if ( StringUtils.isBlank( datetime ) || datetime.equals( "NULL" ) )
            return null;
        LocalDateTime ldt = parseDateTime( datetime ).toLocalDateTime();
        if ( ldt == null ) {
            return null;
        }
        return ldt.atZone( zoneId ).toLocalTime();
    }

    public OffsetDateTime parseDateAsDateTime( String date ) {
        LocalDate ld = parseDate( date );
        if ( ld == null ) {
            return null;
        }
        LocalDateTime ldt = ld.atTime( 0, 0 );
        return ldt.atZone( zoneId ).toOffsetDateTime();
    }

    public LocalTime parseTime( String time ) {
        return parseFromPatterns( time, LocalTime::parse, ( r, str ) -> r );
    }

    public LocalDateTime parseLocalDateTime( String date ) {
        return parseFromPatterns( date, LocalDateTime::parse, ( r, str ) -> r );
    }

    public LocalDate parseDate( String date ) {
        if ( StringUtils.isBlank( date ) || date.equals( "NULL" ) )
            return null;
        return parseFromPatterns(
                date,
                LocalDate::parse,
                ( ld, datePattern ) -> DecadeChangeHelper.fixTwoYearPatternLocalDate( ld, datePattern ) );
    }
}

