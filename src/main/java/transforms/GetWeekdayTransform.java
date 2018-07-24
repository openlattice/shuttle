/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 *
 *
 */

package transforms;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;
import static com.openlattice.shuttle.util.Parsers.getAsString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import org.apache.commons.lang3.StringUtils;

public class GetWeekdayTransform extends Transformation<String> {
    /**
     * Represents a transformation to get weekday from a date.
     */
    @JsonCreator
    public GetWeekdayTransform() {}

     @Override public Object apply( String o ) {
         List<String> days = Arrays
                 .asList( "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY" );
         String dateStr = getAsString( o );
         if ( dateStr != null ) {
             SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd" );
             Date date;
             try {
                 date = dateFormat.parse( dateStr );
                 return days.get( date.getDay() );
             } catch ( Exception e ) {
                 e.printStackTrace();
             }
             return dateStr;
         }
         return null;
     }
}
