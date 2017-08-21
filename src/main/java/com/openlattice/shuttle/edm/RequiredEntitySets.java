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

import java.util.Set;

public class RequiredEntitySets {

    private final FullQualifiedName      type;
    private final Set<EntitySetMetadata> entitySets;

    @JsonCreator
    public RequiredEntitySets(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName type,
            @JsonProperty( SerializationConstants.ENTITY_SETS_FIELD ) Set<EntitySetMetadata> entitySets ) {

        this.type = type;
        this.entitySets = entitySets;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getType() {
        return type;
    }

    @JsonProperty( SerializationConstants.ENTITY_SETS_FIELD )
    public Set<EntitySetMetadata> getEntitySets() {
        return entitySets;
    }
}
