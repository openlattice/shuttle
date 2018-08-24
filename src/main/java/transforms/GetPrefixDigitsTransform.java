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
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

public class GetPrefixDigitsTransform extends Transformation<String> {
    private final String separator;

    /**
     * Represents a transformation to get the digits at the start of a column (if starts with digits).
     *
     * @param separator: separation between digits and
     */
    @JsonCreator
    public GetPrefixDigitsTransform(
            @JsonProperty(Constants.SEP) String separator) {
        this.separator = separator;
    }

    @JsonProperty(Constants.SEP)
    public String getSeparator() {
        return separator;
    }

    @Override
    public Object apply(String o) {
        if (StringUtils.isBlank(o)) {
            return null;
        }
        if (Character.isDigit(o.trim().charAt(0))) {
            String[] strBadge = o.split(separator);
            return Parsers.parseInt(strBadge[0].trim());
        }
        return null;
    }

}

