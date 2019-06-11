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

package com.openlattice.shuttle.transformations;

import com.openlattice.client.serialization.SerializableFunction;

import java.util.List;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TransformValueMapper implements SerializableFunction<Map<String, Object>, Object> {
    private final List<Transformation> transforms;

    public TransformValueMapper( List<Transformation> transforms ) {
        this.transforms = transforms;
    }

    @Override public Object apply( Map<String, Object> input ) {

        if (transforms.size() == 0){
            throw new IllegalStateException( String
                    .format( "At least 1 transformation should be specified (or left blank for columntransform)." ) );
        }

        Object value = input;
        for ( Transformation t : transforms ) {
            value = t.apply( value );
        }
        return value == "" ? null : value;
    }
}
