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
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateTimeHelper implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger( DateTimeHelper.class );

    private final     DateTimeZone      tz;
    private final     String            datePattern;
    private transient DateTimeFormatter formatter;

    public DateTimeHelper( DateTimeZone tz, String datePattern ) {
        this.tz = tz;
        this.datePattern = datePattern;
        this.formatter = DateTimeFormat.forPattern( datePattern );
    }

    public DateTimeHelper( TimeZone tz, String datePattern ) {
        this( DateTimeZone.forTimeZone( tz ), datePattern );
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

        if ( formatter == null ) {
            this.formatter = DateTimeFormat.forPattern( datePattern );
        }

        try {
            return LocalDateTime.parse( date, formatter ).toDateTime( tz );
        } catch ( Exception e ) {
            logger.error( "Unable to parse date {}", date, e );
            return null;
        }
    }

}
