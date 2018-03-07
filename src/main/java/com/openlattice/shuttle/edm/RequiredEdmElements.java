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
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration;

import java.util.Set;

@ReloadableConfiguration( uri = "edm.yaml" )
public class RequiredEdmElements {

    private final Set<RequiredProperties>   propertyTypes;
    private final Set<EntityTypeModel>      entityTypes;
    private final Set<AssociationTypeModel> associationTypes;
    private final Set<EntitySetModel>       entitySets;

    @JsonCreator
    public RequiredEdmElements(
            @JsonProperty( SerializationConstants.PROPERTY_TYPES ) Set<RequiredProperties> propertyTypes,
            @JsonProperty( SerializationConstants.ENTITY_TYPES ) Set<EntityTypeModel> entityTypes,
            @JsonProperty( SerializationConstants.ASSOCIATION_TYPES ) Set<AssociationTypeModel> associationTypes,
            @JsonProperty( SerializationConstants.ENTITY_SETS_FIELD ) Set<EntitySetModel> entitySets ) {

        this.propertyTypes = propertyTypes;
        this.entityTypes = entityTypes;
        this.associationTypes = associationTypes;
        this.entitySets = entitySets;
    }

    @JsonProperty( SerializationConstants.PROPERTY_TYPES )
    public Set<RequiredProperties> getPropertyTypes() {
        return propertyTypes;
    }

    @JsonProperty( SerializationConstants.ENTITY_TYPES )
    public Set<EntityTypeModel> getEntityTypes() {
        return entityTypes;
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_TYPES )
    public Set<AssociationTypeModel> getAssociationTypes() {
        return associationTypes;
    }

    @JsonProperty( SerializationConstants.ENTITY_SETS_FIELD )
    public Set<EntitySetModel> getEntitySets() {
        return entitySets;
    }
}
