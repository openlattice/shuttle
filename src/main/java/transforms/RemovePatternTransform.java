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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@JsonIgnoreProperties(value = {TRANSFORM})

public class RemovePatternTransform extends Transformation<String> {

    private final List<String> patterns;
    private final List<Pattern> rgx;

    /**
     * Represents a transformation to set values to "" if they appear.
     * Can be replaced with replaceregex.
     *
     * @param patterns: list of patterns to remove if they appear (unisolated, so watch out !)
     */
    @JsonCreator
    public RemovePatternTransform(@JsonProperty(Constants.PATTERNS) List<String> patterns) {
        this.patterns = patterns;

        List<Pattern> rgx = new ArrayList<Pattern>();
        for (String ptrn : patterns) {
            rgx.add(
                    Pattern.compile("\\b(?i)" + ptrn + "\\b", Pattern.CASE_INSENSITIVE)
            );
        }
        this.rgx = rgx;

    }

    @JsonProperty(value = Constants.PATTERNS)
    public List<String> getPatterns() {
        return patterns;
    }

    @Override
    public Object apply(String o) {
        for (Pattern rx : rgx) {
            if (StringUtils.isBlank(o)) {
                return null;
            }
            if (rx.matcher(o).matches()) {
                return null;
            }
        }
        return o;
    }

}
