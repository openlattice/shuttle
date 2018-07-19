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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.SetMultimap;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class ColumnTransform extends Transformation<Map<String, String>> {
    private final String column;

    /**
     * Represents a transformation to select a column in the original data (i.e. no transform)
     * @param column: column name to collect
     */
    @JsonCreator
    public ColumnTransform( @JsonProperty( Constants.COLUMN ) String column ) {
        this.column = column;
    }

    @Override public Object apply( Map<String, String> row ) {
        return row.get( column );
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column;
    }
}
