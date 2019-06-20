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

package generators;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;

import java.util.List;
import java.util.Map;

public class TransformSeriesGenerator implements SerializableFunction<Map<String, Object>, String> {
    private final TransformValueMapper transformValueMapper;

    /**
     * Represents a generator to create a String from a combination of transformations
     *
     * @param transforms:   list of transformations
     */
    @JsonCreator
    public TransformSeriesGenerator(
            @JsonProperty( Constants.TRANSFORMS) List<Transformation> transforms
    ) {
        this.transformValueMapper = new TransformValueMapper(transforms);
    }


    @Override
    public String apply( Map<String, Object> row ) {

        return this.transformValueMapper.apply(row).toString();
    }
}
