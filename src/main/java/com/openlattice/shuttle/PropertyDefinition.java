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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.data.integration.StorageDestination;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import transforms.ColumnTransform;
import transforms.HashTransform;

import java.io.Serializable;
import java.util.*;

@JsonInclude( value = Include.NON_EMPTY )
public class PropertyDefinition implements Serializable {

    private static final long serialVersionUID = -6759550320515138785L;

    private final FullQualifiedName                            propertyTypeFqn;
    private final SerializableFunction<Map<String, Object>, ?> valueMapper;
    private final String                                       column;
    private final Optional<Transformations>                    transforms;
    private final Optional<StorageDestination>                 storageDestination;

    @JsonCreator
    public PropertyDefinition(
            @JsonProperty( Constants.TYPE ) String propertyTypeFqn,
            @JsonProperty( Constants.COLUMN ) String column,
            @JsonProperty( Constants.STORAGE_DESTINATION ) Optional<StorageDestination> storageDestination,
            @JsonProperty( Constants.READER ) Optional<Transformation> reader,
            @JsonProperty( Constants.TRANSFORMS ) Optional<Transformations> transforms ) {
        this.propertyTypeFqn = propertyTypeFqn == null ? null : new FullQualifiedName( propertyTypeFqn );
        this.column = column == null ? "" : column;
        this.transforms = transforms;

        if ( transforms.isPresent() ) {
            final List<Transformation> internalTransforms = new ArrayList<>( this.transforms.get().size() + 1 );
            if ( reader.isPresent() ) {
                internalTransforms.add( reader.get() );
            } else if ( column != null ) {
                internalTransforms.add( new ColumnTransform( column ) );
            }
            internalTransforms.addAll( transforms.get() );
            this.valueMapper = new TransformValueMapper( internalTransforms );
        } else {
            this.valueMapper = row -> row.get( column );
        }

        this.storageDestination = storageDestination;
    }

    public PropertyDefinition(
            String propertyTypeFqn,
            String columnName,
            SerializableFunction<Map<String, Object>, ?> valueMapper ) {
        this( propertyTypeFqn, columnName, valueMapper, Optional.empty() );
    }

    public PropertyDefinition(
            String propertyTypeFqn,
            String columnName,
            SerializableFunction<Map<String, Object>, ?> valueMapper,
            Optional<StorageDestination> storageDestination ) {
        this.propertyTypeFqn = new FullQualifiedName( propertyTypeFqn );
        this.valueMapper = valueMapper;
        this.column = columnName;
        this.transforms = Optional.empty();
        this.storageDestination = storageDestination;
    }

    private PropertyDefinition( PropertyDefinition.Builder builder ) {
        this.propertyTypeFqn = builder.propertyTypeFqn;
        this.valueMapper = builder.valueMapper;
        this.column = builder.column;
        this.transforms = Optional.ofNullable( builder.transforms );
        this.storageDestination = builder.storageDestination;
    }

    @JsonProperty( Constants.STORAGE_DESTINATION )
    public Optional<StorageDestination> getStorageDestination() {
        return storageDestination;
    }

    @JsonProperty( Constants.TRANSFORMS )
    public Optional<Transformations> getTransforms() {
        return transforms;
    }

    @JsonProperty( Constants.TYPE )
    public String getType() {
        return this.propertyTypeFqn.getFullQualifiedNameAsString();
    }

    @JsonIgnore
    public FullQualifiedName getFullQualifiedName() {
        return this.propertyTypeFqn;
    }

    @JsonProperty( Constants.COLUMN )
    public String getColumn() {
        return this.column;
    }

    @JsonIgnore
    public SerializableFunction<Map<String, Object>, ?> getPropertyValue() {
        return row -> this.valueMapper.apply( Preconditions.checkNotNull( row ) );
    }

    @Override public String toString() {
        return "PropertyDefinition{" +
                "propertyTypeFqn=" + propertyTypeFqn +
                ", valueMapper=" + valueMapper +
                ", column='" + column + '\'' +
                ", transforms=" + transforms +
                '}';
    }

    @Override public boolean equals( Object o ) {
        if ( this == o ) { return true; }
        if ( o == null || getClass() != o.getClass() ) { return false; }
        PropertyDefinition that = (PropertyDefinition) o;
        return Objects.equals( propertyTypeFqn, that.propertyTypeFqn ) &&
                Objects.equals( column, that.column ) &&
                Objects.equals( transforms, that.transforms );
    }

    @Override public int hashCode() {
        return Objects.hash( propertyTypeFqn, column, transforms );
    }

    public static class Builder<T extends BaseBuilder> extends BaseBuilder<T, PropertyDefinition> {

        private FullQualifiedName                            propertyTypeFqn;
        private SerializableFunction<Map<String, Object>, ?> valueMapper;
        private Transformations                              transforms;
        private String                                       column             = "";
        private Optional<StorageDestination>                 storageDestination = Optional.empty();

        public Builder(
                FullQualifiedName propertyTypeFqn,
                T builder,
                BuilderCallback<PropertyDefinition> builderCallback ) {
            super( builder, builderCallback );
            this.propertyTypeFqn = propertyTypeFqn;
        }

        public Builder<T> value( List<Transformation> transforms ) {
            this.transforms = new Transformations( transforms );
            this.valueMapper = new TransformValueMapper( transforms );
            return this;
        }

        public Builder<T> value( List<String> columns, HashTransform.HashType hashFunction ) {
            return value( ImmutableList.of( new HashTransform( columns, hashFunction ) ) );
        }

        public Builder<T> extractor( SerializableFunction<Map<String, Object>, Object> mapper ) {
            this.valueMapper = mapper;
            return this;
        }

        public Builder<T> value( SerializableFunction<Row, Object> mapper ) {
            this.valueMapper = new RowAdapter( mapper );
            return this;
        }

        public Builder<T> value( String column ) {
            this.column = column;
            this.valueMapper = row -> row.get( column );
            return this;
        }

        public Builder<T> storageDestionation( StorageDestination storageDestination ) {
            this.storageDestination = Optional.of( storageDestination );
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
