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

import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.openlattice.shuttle.adapter.Row;
import java.util.Map;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.io.Serializable;

public class PropertyDefinition implements Serializable {

    private static final long serialVersionUID = -6759550320515138785L;

    private FullQualifiedName                           propertyTypeFqn;
    private SerializableFunction<Map<String,String>, ?> valueMapper;

    @JsonCreator
    public PropertyDefinition(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName propertyTypeFqn,
            @JsonProperty( SerializationConstants.VALUE_MAPPER ) SerializableFunction<Map<String,String>, ?> valueMapper ) {

        this.propertyTypeFqn = propertyTypeFqn;
        this.valueMapper = valueMapper;
    }

    private PropertyDefinition( PropertyDefinition.Builder builder ) {

        this.propertyTypeFqn = builder.propertyTypeFqn;
        this.valueMapper = builder.valueMapper;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getFullQualifiedName() {

        return this.propertyTypeFqn;
    }

    @JsonProperty( SerializationConstants.VALUE_MAPPER )
    public SerializableFunction<Map<String,String>, ?> getPropertyValue() {

        return row -> this.valueMapper.apply( Preconditions.checkNotNull( row ) );
    }

    public static class Builder<T extends BaseBuilder> extends BaseBuilder<T, PropertyDefinition> {

        private FullQualifiedName            propertyTypeFqn;
        private SerializableFunction<Map<String,String>, ?> valueMapper;

        public Builder(
                FullQualifiedName propertyTypeFqn,
                T builder,
                BuilderCallback<PropertyDefinition> builderCallback ) {

            super( builder, builderCallback );
            this.propertyTypeFqn = propertyTypeFqn;
        }

        public Builder<T> value( Funnel<Map<String,String>> funnel ) {
            this.valueMapper = HashingMapper.getMapper( funnel );
            return this;
        }

        public Builder<T> value( Funnel<Map<String,String>> funnel, HashFunction hashFunction ) {
            this.valueMapper = HashingMapper.getMapper( funnel, hashFunction );
            return this;
        }

        public Builder<T> extractor( SerializableFunction<Map<String,String>, Object> mapper ) {
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

    @Override
    public String toString() {

        return "PropertyDefinition [propertyTypeFqn=" + propertyTypeFqn + ", valueMapper=" + valueMapper + "]";
    }

    @Override
    public int hashCode() {

        int result = 1;
        final int prime = 31;
        result = prime * result + ( ( propertyTypeFqn == null ) ? 0 : propertyTypeFqn.hashCode() );
        result = prime * result + ( ( valueMapper == null ) ? 0 : valueMapper.hashCode() );

        return result;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( this == obj ) {
            return true;
        }

        if ( obj == null ) {
            return false;
        }

        if ( !( obj instanceof PropertyDefinition ) ) {
            return false;
        }

        PropertyDefinition other = (PropertyDefinition) obj;

        if ( propertyTypeFqn == null && other.propertyTypeFqn != null ) {
            return false;
        } else if ( !propertyTypeFqn.equals( other.propertyTypeFqn ) ) {
            return false;
        }

        if ( valueMapper == null && other.valueMapper != null ) {
            return false;
        } else if ( !SerializableFunction.class.isAssignableFrom( other.valueMapper.getClass() ) ) {
            return false;
        }

        return true;
    }
}
