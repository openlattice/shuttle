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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.dataloom.client.serialization.SerializableFunction;
import com.dataloom.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityDefinition implements Serializable {

    private static final long serialVersionUID = -3565689091187367622L;

    private static final Logger logger = LoggerFactory.getLogger( EntityDefinition.class );

    private final FullQualifiedName                                           entityTypeFqn;
    private final String                                                      entitySetName;
    private final List<FullQualifiedName>                                     key;
    private final Map<FullQualifiedName, PropertyDefinition>                  propertyDefinitions;
    private final String                                                      alias;
    private final Optional<SerializableFunction<Map<String, String>, String>> generator;
    private final boolean                                                     useCurrentSync;

    @JsonCreator
    public EntityDefinition(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.ENTITY_SET_NAME ) String entitySetName,
            @JsonProperty( SerializationConstants.KEY_FIELD ) List<FullQualifiedName> key,
            @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
                    Map<FullQualifiedName, PropertyDefinition> propertyDefinitions,
            @JsonProperty( SerializationConstants.NAME ) String alias,
            @JsonProperty( SerializationConstants.ENTITY_ID_GENERATOR )
                    Optional<SerializableFunction<Map<String, String>, String>> generator,
            @JsonProperty( SerializationConstants.CURRENT_SYNC ) Optional<Boolean> useCurrentSync ) {

        this.entityTypeFqn = entityTypeFqn;
        this.entitySetName = entitySetName;
        this.propertyDefinitions = propertyDefinitions;
        this.key = key;
        this.alias = alias;
        this.generator = generator;
        this.useCurrentSync = useCurrentSync.or( false );
    }

    private EntityDefinition( EntityDefinition.Builder builder ) {

        this.entityTypeFqn = builder.entityTypeFqn;
        this.entitySetName = builder.entitySetName;
        this.propertyDefinitions = builder.propertyDefinitionMap;
        this.key = builder.key;
        this.alias = builder.alias;
        this.generator = Optional.fromNullable( builder.entityIdGenerator );
        this.useCurrentSync = builder.useCurrentSync;
    }

    @JsonProperty( SerializationConstants.TYPE_FIELD )
    public FullQualifiedName getEntityTypeFqn() {
        return this.entityTypeFqn;
    }

    @JsonProperty( SerializationConstants.ENTITY_SET_NAME )
    public String getEntitySetName() {
        return this.entitySetName;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public List<FullQualifiedName> getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
    public Map<FullQualifiedName, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    @JsonProperty( SerializationConstants.NAME )
    public String getAlias() {
        return alias;
    }

    @JsonProperty( SerializationConstants.ENTITY_ID_GENERATOR )
    public Optional<SerializableFunction<Map<String, String>, String>> getGenerator() {
        return generator;
    }

    @JsonProperty( SerializationConstants.CURRENT_SYNC )
    public boolean useCurrentSync() {
        return useCurrentSync;
    }

    @JsonIgnore
    public Collection<PropertyDefinition> getProperties() {
        return this.propertyDefinitions.values();
    }

    @Override
    public String toString() {
        return "EntityDefinition [entityTypeFqn=" + entityTypeFqn + ", entitySetName=" + entitySetName
                + ", propertyDefinitions=" + propertyDefinitions + ", key=" + key + ", alias=" + alias + ", generator="
                + generator + "]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( alias == null ) ? 0 : alias.hashCode() );
        result = prime * result + ( ( entitySetName == null ) ? 0 : entitySetName.hashCode() );
        result = prime * result + ( ( entityTypeFqn == null ) ? 0 : entityTypeFqn.hashCode() );
        result = prime * result + ( ( generator == null ) ? 0 : generator.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( propertyDefinitions == null ) ? 0 : propertyDefinitions.hashCode() );
        return result;
    }

    @Override
    public boolean equals( Object obj ) {

        if ( this == obj ) { return true; }
        if ( obj == null ) { return false; }
        if ( getClass() != obj.getClass() ) { return false; }
        EntityDefinition other = (EntityDefinition) obj;
        if ( alias == null ) {
            if ( other.alias != null ) { return false; }
        } else if ( !alias.equals( other.alias ) ) { return false; }
        if ( entitySetName == null ) {
            if ( other.entitySetName != null ) { return false; }
        } else if ( !entitySetName.equals( other.entitySetName ) ) { return false; }
        if ( entityTypeFqn == null ) {
            if ( other.entityTypeFqn != null ) { return false; }
        } else if ( !entityTypeFqn.equals( other.entityTypeFqn ) ) { return false; }
        if ( generator == null ) {
            if ( other.generator != null ) { return false; }
        } else if ( !generator.equals( other.generator ) ) { return false; }
        if ( key == null ) {
            if ( other.key != null ) { return false; }
        } else if ( !key.equals( other.key ) ) { return false; }
        if ( propertyDefinitions == null ) {
            if ( other.propertyDefinitions != null ) { return false; }
        } else if ( !propertyDefinitions.equals( other.propertyDefinitions ) ) { return false; }
        return true;
    }

    public static class Builder extends BaseBuilder<EntityGroup.Builder, EntityDefinition> {

        private FullQualifiedName                                 entityTypeFqn;
        private String                                            entitySetName;
        private List<FullQualifiedName>                           key;
        private Map<FullQualifiedName, PropertyDefinition>        propertyDefinitionMap;
        private String                                            alias;
        private SerializableFunction<Map<String, String>, String> entityIdGenerator;
        private boolean                                           useCurrentSync;

        public Builder(
                String alias,
                EntityGroup.Builder builder,
                BuilderCallback<EntityDefinition> builderCallback ) {

            super( builder, builderCallback );

            this.alias = alias;
            this.propertyDefinitionMap = Maps.newHashMap();
            this.useCurrentSync = false;
        }

        public Builder key( String... key ) {
            return key(
                    Stream.of( key ).map( FullQualifiedName::new ).toArray( FullQualifiedName[]::new )
            );
        }

        public Builder key( FullQualifiedName... key ) {

            checkNotNull( key, "Key cannot be null." );
            checkArgument(
                    ImmutableSet.copyOf( key ).size() == key.length,
                    "Key must be a set of unique FQNs"
            );

            this.key = Arrays.asList( key );
            return this;
        }

        public Builder to( String entitySetName ) {
            this.entitySetName = entitySetName;
            return this;
        }

        public Builder ofType( String fqn ) {
            return ofType( new FullQualifiedName( fqn ) );
        }

        public Builder ofType( FullQualifiedName entityTypeFqn ) {
            this.entityTypeFqn = entityTypeFqn;
            return this;
        }

        public Builder entityIdGenerator(
                SerializableFunction<Map<String, String>, String> generator ) {
            this.entityIdGenerator = generator;
            return this;
        }

        public Builder useCurrentSync() {
            this.useCurrentSync = true;
            return this;
        }

        public PropertyDefinition.Builder<EntityDefinition.Builder> addProperty( String propertyTypeFqn ) {
            return addProperty( new FullQualifiedName( propertyTypeFqn ) );
        }

        public PropertyDefinition.Builder<EntityDefinition.Builder> addProperty( FullQualifiedName propertyTypeFqn ) {

            BuilderCallback<PropertyDefinition> onBuild = propertyDefinition -> {
                FullQualifiedName propertyDefFqn = propertyDefinition.getFullQualifiedName();
                if ( propertyDefinitionMap.containsKey( propertyDefFqn ) ) {
                    throw new IllegalStateException(
                            String.format( "encountered duplicate property: %s", propertyDefFqn ) );
                }
                propertyDefinitionMap.put( propertyDefFqn, propertyDefinition );
            };

            return new PropertyDefinition.Builder<EntityDefinition.Builder>( propertyTypeFqn, this, onBuild );
        }

        public Builder addProperty( String propertyFqn, String columnName ) {
            return addProperty( new FullQualifiedName( propertyFqn ), columnName );
        }

        public Builder addProperty( FullQualifiedName propertyFqn, String columnName ) {
            SerializableFunction<Map<String, String>, ?> defaultMapper = row -> row.get( columnName );
            PropertyDefinition propertyDefinition = new PropertyDefinition( propertyFqn, defaultMapper );
            this.propertyDefinitionMap.put( propertyFqn, propertyDefinition );
            return this;
        }

        public EntityGroup.Builder ok() {
            return endEntity();
        }

        public EntityGroup.Builder endEntity() {

            if ( this.propertyDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking addProperty() at least once is required" );
            }

            return super.ok( new EntityDefinition( this ) );
        }
    }
}
