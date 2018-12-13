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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.client.serialization.SerializationConstants;
import com.openlattice.shuttle.conditions.Condition;
import com.openlattice.shuttle.conditions.ConditionValueMapper;
import com.openlattice.shuttle.conditions.Conditions;
import com.openlattice.shuttle.transformations.TransformValueMapper;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.util.Constants;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import transforms.ColumnTransform;

@JsonInclude( value = Include.NON_EMPTY )
public class EntityDefinition implements Serializable {

    private static final long serialVersionUID = -3565689091187367622L;

    private static final Logger logger = LoggerFactory.getLogger( EntityDefinition.class );

    private final FullQualifiedName                                           entityTypeFqn;
    private final String                                                      entitySetName;
    private final List<FullQualifiedName>                                     key;
    private final Map<FullQualifiedName, PropertyDefinition>                  propertyDefinitions;
    private final String                                                      alias;
    public final  Optional<Conditions>                                        condition;
    private final Optional<SerializableFunction<Map<String, Object>, String>> generator;
    private final boolean                                                     useCurrentSync;
    public final  SerializableFunction<Map<String, Object>, ?>                valueMapper;

    @JsonCreator
    public EntityDefinition(
            @JsonProperty( SerializationConstants.FQN ) String entityTypeFqn,
            @JsonProperty( SerializationConstants.ENTITY_SET_NAME ) String entitySetName,
            @JsonProperty( SerializationConstants.KEY_FIELD ) List<FullQualifiedName> key,
            @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
                    Map<FullQualifiedName, PropertyDefinition> propertyDefinitions,
            @JsonProperty( SerializationConstants.NAME ) String alias,
            @JsonProperty( Constants.CONDITIONS ) Optional<Conditions> condition,
            @JsonProperty( SerializationConstants.CURRENT_SYNC ) Boolean useCurrentSync ) {

        this.entityTypeFqn = entityTypeFqn == null ? null : new FullQualifiedName( entityTypeFqn );
        this.entitySetName = entitySetName;
        this.propertyDefinitions = propertyDefinitions;
        this.key = key;
        this.alias = alias == null ? entitySetName : alias;
        this.generator = Optional.empty();
        this.condition = condition;
        this.useCurrentSync = useCurrentSync == null ? false : useCurrentSync;

        if ( condition.isPresent() ) {
            final List<Condition> internalConditions;
            internalConditions = new ArrayList<>( this.condition.get().size() + 1 );
            condition.get().forEach( internalConditions::add );
            this.valueMapper = new ConditionValueMapper( internalConditions );
        } else {
            this.valueMapper = null;
        }
    }

    public EntityDefinition(
            String entityTypeFqn,
            String entitySetName,
            List<FullQualifiedName> key,
            Map<FullQualifiedName, PropertyDefinition> propertyDefinitions,
            Optional<SerializableFunction<Map<String, Object>, String>> generator,
            String alias,
            Optional<Boolean> useCurrentSync
    ) {

        this.entityTypeFqn = new FullQualifiedName( entityTypeFqn );
        this.entitySetName = entitySetName;
        this.propertyDefinitions = propertyDefinitions;
        this.key = key;
        this.alias = alias;
        this.generator = generator;
        this.condition = Optional.empty();
        this.valueMapper = null;
        this.useCurrentSync = useCurrentSync.orElse( false );
    }

    private EntityDefinition( EntityDefinition.Builder builder ) {

        this.entityTypeFqn = builder.entityTypeFqn;
        this.entitySetName = builder.entitySetName;
        this.propertyDefinitions = builder.propertyDefinitionMap;
        this.key = builder.key;
        this.condition = Optional.empty();
        this.valueMapper = null;
        this.alias = builder.alias;
        this.generator = Optional.ofNullable( builder.entityIdGenerator );
        this.useCurrentSync = builder.useCurrentSync;
    }

    @JsonIgnore
    //    @JsonProperty( SerializationConstants.FQN )
    public FullQualifiedName getEntityTypeFqn() {
        return this.entityTypeFqn;
    }

    @JsonProperty( SerializationConstants.FQN )
    public String getFqn() {
        return this.entityTypeFqn == null ? null : this.entityTypeFqn.getFullQualifiedNameAsString();
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
    public Optional<SerializableFunction<Map<String, Object>, String>> getGenerator() {
        return generator;
    }

    @JsonProperty( SerializationConstants.CURRENT_SYNC )
    public boolean useCurrentSync() {
        return useCurrentSync;
    }

    @JsonProperty( Constants.CONDITIONS )
    public Optional<Conditions> getCondition() {
        return condition;
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
        private SerializableFunction<Map<String, Object>, String> entityIdGenerator;
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
                SerializableFunction<Map<String, Object>, String> generator ) {
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

            return new PropertyDefinition.Builder<>( propertyTypeFqn, this, onBuild );
        }
        public Builder addProperty( String propertyString, String columnName ) {
            // This function is for when flights are defined in java
            // Useful for backwards compatibility
            FullQualifiedName propertyFqn = new FullQualifiedName( propertyString );
            SerializableFunction<Map<String, Object>, ?> defaultMapper = row -> {
                String value = row.get( columnName ).toString();
                return ( value instanceof String && StringUtils.isBlank( value ) ) ? null : value;
            };
            PropertyDefinition propertyDefinition = new PropertyDefinition(
                    propertyString, columnName, defaultMapper );
            this.propertyDefinitionMap.put( propertyFqn, propertyDefinition );
            return this;
        }

        public Builder addProperty(
                String propertyString,
                String columnName,
                Transformations transformation ) {
            FullQualifiedName propertyFqn = new FullQualifiedName( propertyString );
            PropertyDefinition propertyDefinition = new PropertyDefinition(
                    propertyString, columnName, Optional.empty(), Optional.of( transformation ) );
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
