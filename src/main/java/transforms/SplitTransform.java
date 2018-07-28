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
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SplitTransform extends Transformation<String> {

    private final String separator;
    private final String valueelse;
    private final int    index;

    /**
     * Represents a transformation to split a string
     *
     * @param separator: separate by what?
     * @param index:     index in separated list (starts at 0!)
     */
    @JsonCreator
    public SplitTransform(
            @JsonProperty( Constants.SEP ) String separator,
            @JsonProperty( Constants.INDEX ) int index,
            @JsonProperty( Constants.ELSE ) String valueelse
    ) {
        this.separator = separator;
        this.index = index;
        this.valueelse = valueelse == null ? "" : valueelse;
    }

    @Override
    public Object apply( String o ) {
        if ( StringUtils.isBlank( o ) ) {
            return "";
        }
        String[] strNames = o.trim().split( separator );
        if ( strNames.length > index ) {
            return strNames[ index ].trim();
        }
        if ( !StringUtils.isBlank( valueelse ) ) {
            return valueelse;
        }
        return o;
    }

}
