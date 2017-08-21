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

import com.dataloom.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.olingo.commons.api.edm.FullQualifiedName;

import java.util.LinkedHashSet;

public class AssociationTypeModel {

    private final EntityTypeModel                  type;
    private final LinkedHashSet<FullQualifiedName> src;
    private final LinkedHashSet<FullQualifiedName> dst;
    private final boolean                          bidirectional;

    @JsonCreator
    public AssociationTypeModel(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) EntityTypeModel associationEntityType,
            @JsonProperty( SerializationConstants.SRC ) LinkedHashSet<String> src,
            @JsonProperty( SerializationConstants.DST ) LinkedHashSet<String> dst,
            @JsonProperty( SerializationConstants.BIDIRECTIONAL ) boolean bidirectional ) {

        this.type = associationEntityType;
        this.src = new LinkedHashSet<>( src.size() );
        this.dst = new LinkedHashSet<>( dst.size() );
        this.bidirectional = bidirectional;

        src.stream().map( FullQualifiedName::new ).forEachOrdered( this.src::add );
        dst.stream().map( FullQualifiedName::new ).forEachOrdered( this.dst::add );
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public EntityTypeModel getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.BIDIRECTIONAL )
    public boolean isBidirectional() {
        return bidirectional;
    }

    @JsonProperty( SerializationConstants.DST )
    public LinkedHashSet<FullQualifiedName> getDst() {
        return dst;
    }

    @JsonProperty( SerializationConstants.SRC )
    public LinkedHashSet<FullQualifiedName> getSrc() {
        return src;
    }

    @Override
    public boolean equals( Object o ) {

        if ( this == o )
            return true;
        if ( !( o instanceof AssociationTypeModel ) )
            return false;

        AssociationTypeModel that = (AssociationTypeModel) o;

        if ( bidirectional != that.bidirectional )
            return false;
        if ( !type.equals( that.type ) )
            return false;
        if ( !dst.equals( that.dst ) )
            return false;
        return src.equals( that.src );
    }

    @Override
    public int hashCode() {

        int result = type.hashCode();
        result = 31 * result + ( bidirectional ? 1 : 0 );
        result = 31 * result + dst.hashCode();
        result = 31 * result + src.hashCode();
        return result;
    }
}
