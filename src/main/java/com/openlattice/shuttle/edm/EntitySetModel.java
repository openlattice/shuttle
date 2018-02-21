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
import com.google.common.collect.ImmutableSet;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.Set;

public class EntitySetModel {

    private final FullQualifiedName type;
    private final String            name;
    private final String            title;
    private final String            description;
    private final Set<String>       contacts;
    private final Set<String>       owners;


    @JsonCreator
    public EntitySetModel(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) String type,
            @JsonProperty( SerializationConstants.NAME_FIELD ) String name,
            @JsonProperty( SerializationConstants.TITLE_FIELD ) String title,
            @JsonProperty( SerializationConstants.DESCRIPTION_FIELD ) Optional<String> description,
            @JsonProperty( SerializationConstants.CONTACTS ) Set<String> contacts,
            @JsonProperty( "owners" ) Optional<Set<String>> owners ) {

        this.type = new FullQualifiedName( type );
        this.name = name;
        this.title = title;
        this.description = description.or( "" );
        this.contacts = contacts;
        this.owners = owners.or( ImmutableSet::of );
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.NAME_FIELD )
    public String getName() {
        return name;
    }

    @JsonProperty( SerializationConstants.TITLE_FIELD )
    public String getTitle() {
        return title;
    }

    @JsonProperty( SerializationConstants.DESCRIPTION_FIELD )
    public String getDescription() {
        return description;
    }

    @JsonProperty( SerializationConstants.CONTACTS )
    public Set<String> getContacts() {
        return contacts;
    }

    @JsonProperty( "owners" )
    public Set<String> getOwners() {
        return owners;
    }

    @Override
    public boolean equals( Object o ) {

        if ( this == o )
            return true;
        if ( !( o instanceof EntitySetModel ) )
            return false;

        EntitySetModel that = (EntitySetModel) o;

        if ( !type.equals( that.type ) )
            return false;
        if ( !name.equals( that.name ) )
            return false;
        if ( !title.equals( that.title ) )
            return false;
        if ( !description.equals( that.description ) )
            return false;
        if (!contacts.equals( that.contacts ) )
            return false;
        if (!owners.equals( that.owners ) )
            return false;
        return true;
    }

    @Override
    public int hashCode() {

        int result = type.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + title.hashCode();
        result = 31 * result + description.hashCode();
        result = 31 * result + contacts.hashCode();
        result = 31 * result + owners.hashCode();
        return result;
    }
}
