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

package com.openlattice.shuttle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.util.Constants;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

public class PropertyDefinition implements Serializable {

    private static final long serialVersionUID = -6759550320515138785L;

    private FullQualifiedName                            propertyTypeFqn;
    private SerializableFunction<Map<String, String>, ?> valueMapper;
    private String                                       column;

    @JsonCreator
    public PropertyDefinition(
            @JsonProperty( SerializationConstants.FQN ) String propertyTypeFqn,
            @JsonProperty( Constants.TRANSFORMS ) List<Transformation<Object,Object>> transforms,
            @JsonProperty( Constants.COLUMN ) String column ) {
        this.propertyTypeFqn = new FullQualifiedName( propertyTypeFqn );
        this.column = column;
        this.valueMapper = TransformValueMapper.from( row -> row.get( column ), transforms );
    }

    public PropertyDefinition( String propertyTypeFqn, SerializableFunction<Map<String, String>, ?> valueMapper ) {
        this.propertyTypeFqn = new FullQualifiedName( propertyTypeFqn );
        this.valueMapper = valueMapper;
    }

    private PropertyDefinition( PropertyDefinition.Builder builder ) {
        this.propertyTypeFqn = builder.propertyTypeFqn;
        this.valueMapper = builder.valueMapper;
    }

    @JsonProperty( SerializationConstants.FQN )
    public FullQualifiedName getFullQualifiedName() {
        return this.propertyTypeFqn;
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return this.column;
    }

    public SerializableFunction<Map<String, String>, ?> getPropertyValue() {
        return row -> this.valueMapper.apply( Preconditions.checkNotNull( row ) );
    }

    @Override public String toString() {
        return "PropertyDefinition{" +
                "propertyTypeFqn=" + propertyTypeFqn +
                ", valueMapper=" + valueMapper +
                ", column='" + column + '\'' +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( !( o instanceof PropertyDefinition ) ) { return false; }
        PropertyDefinition that = (PropertyDefinition) o;
        return Objects.equals( propertyTypeFqn, that.propertyTypeFqn ) &&
                Objects.equals( valueMapper, that.valueMapper ) &&
                Objects.equals( column, that.column );
    }

    @Override public int hashCode() {
        return Objects.hash( propertyTypeFqn, valueMapper, column );
    }

    public static class Builder<T extends BaseBuilder> extends BaseBuilder<T, PropertyDefinition> {

        private FullQualifiedName                            propertyTypeFqn;
        private SerializableFunction<Map<String, String>, ?> valueMapper;

        public Builder(
                FullQualifiedName propertyTypeFqn,
                T builder,
                BuilderCallback<PropertyDefinition> builderCallback ) {

            super( builder, builderCallback );
            this.propertyTypeFqn = propertyTypeFqn;
        }

        public Builder<T> value( Funnel<Map<String, String>> funnel ) {
            this.valueMapper = HashingMapper.getMapper( funnel );
            return this;
        }

        public Builder<T> value( Funnel<Map<String, String>> funnel, HashFunction hashFunction ) {
            this.valueMapper = HashingMapper.getMapper( funnel, hashFunction );
            return this;
        }

        public Builder<T> extractor( SerializableFunction<Map<String, String>, Object> mapper ) {
            this.valueMapper = mapper;
            return this;
        }

        public Builder<T> value( SerializableFunction<Row, Object> mapper ) {
            this.valueMapper = new RowAdapter( mapper );
            return this;
        }

        public Builder<T> value( String column ) {
            this.valueMapper = row -> row.get( column );
            return this;
        }

        public T ok() {

            if ( this.valueMapper == null ) {
                throw new IllegalStateException( "invoking value() is required" );
            }

            return super.ok( new PropertyDefinition( this ) );
        }
    }

}
