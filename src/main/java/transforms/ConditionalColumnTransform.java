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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonIgnoreProperties(value = {TRANSFORM} )
public class ConditionalColumnTransform extends Transformation<Map<String, String>> {
    private final List<String> columns;

    /**
     * Represents a transformation to select columns based on non-empty cells.
     * Function goes over columns until a non-zero input is found.
     * @param columns: list of columns to go over in sequential order
     */
     @JsonCreator
     public ConditionalColumnTransform(
            @JsonProperty( Constants.COLUMNS ) List<String> columns) {
        this.columns = columns;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override public Object apply( Map<String, String> row ) {
        for ( String s : columns ) {
            String thiscol = row.get(s);
            if ( StringUtils.isNotBlank(thiscol)) {
                return thiscol;
            }
        }
        return "";
    }

}

