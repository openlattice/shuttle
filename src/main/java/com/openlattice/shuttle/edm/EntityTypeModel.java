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

package com.openlattice.shuttle.edm;

import com.openlattice.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.LinkedHashSet;

public class EntityTypeModel {

    private final FullQualifiedName                type;
    private final String                           title;
    private final String                           description;
    private final LinkedHashSet<FullQualifiedName> key;
    private final LinkedHashSet<FullQualifiedName> properties;
    private final Optional<FullQualifiedName>      baseType;

    @JsonCreator
    public EntityTypeModel(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) String type,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.KEY_FIELD ) LinkedHashSet<String> key,
            @JsonProperty( SerializationConstants.PROPERTIES_FIELD ) LinkedHashSet<String> properties,
            @JsonProperty( SerializationConstants.BASE_TYPE_FIELD ) Optional<String> baseType ) {

        this.type = new FullQualifiedName( type );
        this.title = title;
        this.description = description.or( "" );
        this.key = new LinkedHashSet<>( key.size() );
        this.properties = new LinkedHashSet<>( properties.size() );
        this.baseType = baseType.transform( FullQualifiedName::new );

        key.stream().map( FullQualifiedName::new ).forEachOrdered( this.key::add );
        properties.stream().map( FullQualifiedName::new ).forEachOrdered( this.properties::add );
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public LinkedHashSet<FullQualifiedName> getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.PROPERTIES_FIELD )
    public LinkedHashSet<FullQualifiedName> getProperties() {
        return properties;
    }

    @JsonProperty( SerializationConstants.BASE_TYPE_FIELD )
    public Optional<FullQualifiedName> getBaseType() {
        return baseType;
    }

    @Override
    public boolean equals( Object o ) {

        if ( this == o )
            return true;
        if ( !( o instanceof EntityTypeModel ) )
            return false;

        EntityTypeModel that = (EntityTypeModel) o;

        if ( type != null ? !type.equals( that.type ) : that.type != null )
            return false;
        if ( title != null ? !title.equals( that.title ) : that.title != null )
            return false;
        if ( description != null ? !description.equals( that.description ) : that.description != null )
            return false;
        if ( key != null ? !key.equals( that.key ) : that.key != null )
            return false;
        if ( properties != null ? !properties.equals( that.properties ) : that.properties != null )
            return false;
        return baseType != null ? baseType.equals( that.baseType ) : that.baseType == null;
    }

    @Override
    public int hashCode() {

        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + ( title != null ? title.hashCode() : 0 );
        result = 31 * result + ( description != null ? description.hashCode() : 0 );
        result = 31 * result + ( key != null ? key.hashCode() : 0 );
        result = 31 * result + ( properties != null ? properties.hashCode() : 0 );
        result = 31 * result + ( baseType != null ? baseType.hashCode() : 0 );
        return result;
    }
}
