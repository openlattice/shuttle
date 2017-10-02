/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 */

package com.openlattice.shuttle.dates;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeHelper implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger( DateTimeHelper.class );

    private final     DateTimeZone            tz;
    private final     String[]                datePatterns;
    private transient List<DateTimeFormatter> formatters;

    public DateTimeHelper( DateTimeZone tz, String... datePatterns ) {
        this.tz = tz;
        this.datePatterns = datePatterns;
        formatters = Arrays.asList( datePatterns )
                .stream()
                .map( DateTimeFormat::forPattern )
                .collect( Collectors.toList() );
    }

    public DateTimeHelper( TimeZone tz, String... datePatterns ) {
        this( DateTimeZone.forTimeZone( tz ), datePatterns );
    }

    public String parse( String date ) {
        DateTime ldt = parseDT( date );
        return ldt == null ? null : ldt.toString();
    }

    public DateTime parseDT( String date ) {
        if ( date == null ) {
            return null;
        } else if ( date.equals( "NULL" ) ) {
            return null;
        }
        for ( int i = 0; i < datePatterns.length; ++i ) {
            DateTimeFormatter formatter = formatters.get( i );
            if ( formatter == null ) {
                formatter = DateTimeFormat.forPattern( datePatterns[ i ] );
                formatters.set( i, formatter );
            }

            try {
                return LocalDateTime.parse( date, formatter ).toDateTime( tz );
            } catch ( Exception e ) {
                logger.debug( "Unable to parse date {} with format string {}", date, datePatterns[ i ], e );
            }
        }
        logger.error( "Unable to parse date {}, please see debug log for additional information." );
        return null;
    }

}
