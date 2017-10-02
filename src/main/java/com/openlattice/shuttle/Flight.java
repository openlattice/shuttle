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

import com.dataloom.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

public class Flight implements Serializable {

    private static final long serialVersionUID = 2207339044078175121L;

    private final Map<String, EntityDefinition>      entityDefinitions;
    private final Map<String, AssociationDefinition> associationDefinitions;
    private String name ="Anon";

    @JsonCreator
    public Flight(
            @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
                    Map<String, EntityDefinition> entityDefinitions,
            @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
                    Map<String, AssociationDefinition> associationDefinitions ) {
        this.entityDefinitions = entityDefinitions;
        this.associationDefinitions = associationDefinitions;
    }

    private Flight( Flight.Builder builder ) {
        this.entityDefinitions = builder.entityDefinitionMap;
        this.associationDefinitions = builder.associationDefinitionMap;
        this.name = builder.name;
    }

    public static Flight.Builder newFlight() {
        return new Flight.Builder();
    }

    public static Flight.Builder newFlight(String name) {
        return new Flight.Builder(name);
    }

    public String getName() {
        return name;
    }

    @JsonIgnore
    public Collection<EntityDefinition> getEntities() {
        return this.entityDefinitions.values();
    }

    @JsonProperty( SerializationConstants.ENTITY_DEFINITIONS_FIELD )
    public Map<String, EntityDefinition> getEntityDefinitions() {
        return entityDefinitions;
    }

    @JsonIgnore
    public Collection<AssociationDefinition> getAssociations() {
        return this.associationDefinitions.values();
    }

    @JsonProperty( SerializationConstants.ASSOCIATION_DEFINITIONS_FIELD )
    public Map<String, AssociationDefinition> getAssociationDefinitions() {
        return associationDefinitions;
    }

    public static class Builder {

        private Map<String, EntityDefinition>      entityDefinitionMap;
        private Map<String, AssociationDefinition> associationDefinitionMap;
        private String name;

        public Builder() {
            this.entityDefinitionMap = Maps.newHashMap();
            this.associationDefinitionMap = Maps.newHashMap();
        }

        public Builder(String name) {
            this.entityDefinitionMap = Maps.newHashMap();
            this.associationDefinitionMap = Maps.newHashMap();
            this.name = name;
        }


        public EntityGroup.Builder createEntities() {

            BuilderCallback<EntityGroup> onBuild = entities ->
                    this.entityDefinitionMap = entities.getEntityDefinitions();

            return new EntityGroup.Builder( this, onBuild );
        }

        public AssociationGroup.Builder createAssociations() {

            BuilderCallback<AssociationGroup> onBuild = associations ->
                    this.associationDefinitionMap = associations.getAssociationDefinitions();

            return new AssociationGroup.Builder( entityDefinitionMap.keySet(), this, onBuild );
        }

        public Flight done() {

            if ( this.entityDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking createEntities() at least once is required" );
            }

            return new Flight( this );
        }
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( associationDefinitions == null ) ? 0 : associationDefinitions.hashCode() );
        result = prime * result + ( ( entityDefinitions == null ) ? 0 : entityDefinitions.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        Flight other = (Flight) obj;
        if ( associationDefinitions == null ) {
            if ( other.associationDefinitions != null )
                return false;
        } else if ( !associationDefinitions.equals( other.associationDefinitions ) )
            return false;
        if ( entityDefinitions == null ) {
            if ( other.entityDefinitions != null )
                return false;
        } else if ( !entityDefinitions.equals( other.entityDefinitions ) )
            return false;
        return true;
    }

}
