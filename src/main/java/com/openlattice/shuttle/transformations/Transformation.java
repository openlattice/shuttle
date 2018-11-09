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

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.shuttle.util.Constants;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

@JsonTypeInfo( use = Id.CLASS, include = As.PROPERTY, property = TRANSFORM )
public abstract class Transformation<I extends Object> implements Function<I, Object> {
    public static final String TRANSFORM = "@transform";

    private final Optional<String> column;

    public Transformation( Optional<String> column) {
        this.column = column;
    }

    public Transformation() {
        this.column = Optional.empty();
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return column.orElse( null );
    }

    protected String getInputString( Object o, Optional<String> column ) {
        final String input;
        if (o==null){
            return null;
        }
        if (!(column.isPresent())) {
            input = o.toString();
        } else {
            ObjectMapper m = new ObjectMapper();
            Map<String, String> row = m.convertValue( o, Map.class );
            input = row.get(getColumn());
        }

        return input;
    }

    protected Object applyValue( String s ) {
        return s;
    }

    @Override
    public Object apply( I o ) {

        return applyValue( getInputString( o, column ) );
    }
}
