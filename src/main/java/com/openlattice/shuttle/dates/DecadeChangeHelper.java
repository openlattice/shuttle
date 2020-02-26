package com.openlattice.shuttle.dates;

import com.openlattice.shuttle.util.Cached;
import com.openlattice.shuttle.util.Constants;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

public class DecadeChangeHelper {
    /**
     *
     * Dealing with the fact that a two-year year-pattern should not all be in 20YY
     *
     */
    public static boolean checkDatePatternIsTwoDigitYear( String datePattern ) {
        boolean yyMatch = Cached.getMatcherForString( datePattern, ".*yy.*" ).matches();
        boolean yyyyMatch = Cached.getMatcherForString( datePattern, ".*yyyy.*" ).matches();
        return yyMatch && !yyyyMatch;
    }

    public static LocalDateTime fixTwoYearPatternLocalDate (
            LocalDateTime parsedDateTime,
            String datePattern
    ) {
        if ( checkDatePatternIsTwoDigitYear( datePattern ) ) {
            if ( ( parsedDateTime.getYear() - LocalDate.now().getYear() ) > Constants.DECADE_CUTOFF ) {
                parsedDateTime = parsedDateTime.withYear( parsedDateTime.getYear() - Constants.ERA_CUTOFF );
            }
        }
        return parsedDateTime;
    }

    public static LocalDate fixTwoYearPatternLocalDate (
            LocalDate parsedDate,
            String datePattern
    ) {
        if ( checkDatePatternIsTwoDigitYear( datePattern ) ) {
            if ( ( parsedDate.getYear() - LocalDate.now().getYear() ) > Constants.DECADE_CUTOFF ) {
                parsedDate = parsedDate.withYear( parsedDate.getYear() - Constants.ERA_CUTOFF );
            }
        }
        return parsedDate;
    }
    public static OffsetDateTime fixTwoYearPatternOffsetDateTime (
            OffsetDateTime parsedDateTime,
            String datePattern
    ) {
        if ( checkDatePatternIsTwoDigitYear( datePattern ) ) {
            if ( ( parsedDateTime.getYear() - LocalDate.now().getYear() ) > Constants.DECADE_CUTOFF ) {
                parsedDateTime = parsedDateTime.withYear( parsedDateTime.getYear() - Constants.ERA_CUTOFF );
            }
        }
        return parsedDateTime;
    }
}
