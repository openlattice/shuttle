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

import com.dataloom.client.serialization.SerializableFunction;
import com.dataloom.client.serialization.SerializationConstants;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class AssociationDefinition implements Serializable {

    private static final long serialVersionUID = -6632902802080642647L;

    private static final Logger logger = LoggerFactory.getLogger( AssociationDefinition.class );

    private final FullQualifiedName                           entityTypeFqn;
    private final String                                      entitySetName;
    private final List<FullQualifiedName>                     key;
    private final String                                      srcAlias;
    private final String                                      dstAlias;
    private final Map<FullQualifiedName, PropertyDefinition>  propertyDefinitions;
    private final String                                      alias;
    private final Optional<SerializableFunction<Row, String>> generator;
    private final boolean                                     useCurrentSync;

    @JsonCreator
    public AssociationDefinition(
            @JsonProperty( SerializationConstants.TYPE_FIELD ) FullQualifiedName entityTypeFqn,
            @JsonProperty( SerializationConstants.ENTITY_SET_NAME ) String entitySetName,
            @JsonProperty( SerializationConstants.KEY_FIELD ) List<FullQualifiedName> key,
            @JsonProperty( SerializationConstants.SRC ) String srcAlias,
            @JsonProperty( SerializationConstants.DST ) String dstAlias,
            @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
                    Map<FullQualifiedName, PropertyDefinition> propertyDefinitions,
            @JsonProperty( SerializationConstants.NAME ) String alias,
            @JsonProperty( SerializationConstants.ENTITY_ID_GENERATOR )
                    Optional<SerializableFunction<Row, String>> generator,
            @JsonProperty( SerializationConstants.CURRENT_SYNC ) Optional<Boolean> useCurrentSync ) {

        this.entityTypeFqn = entityTypeFqn;
        this.entitySetName = entitySetName;
        this.srcAlias = srcAlias;
        this.dstAlias = dstAlias;
        this.propertyDefinitions = propertyDefinitions;
        this.key = key;
        this.alias = alias;
        this.generator = generator;
        this.useCurrentSync = useCurrentSync.or( false );
    }

    private AssociationDefinition( AssociationDefinition.Builder builder ) {

        this.entityTypeFqn = builder.entityTypeFqn;
        this.entitySetName = builder.entitySetName;
        this.srcAlias = builder.srcAlias;
        this.dstAlias = builder.dstAlias;
        this.propertyDefinitions = builder.propertyDefinitionMap;
        this.key = builder.key;
        this.alias = builder.alias;
        this.generator = Optional.fromNullable( builder.generator );
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

    @JsonProperty( SerializationConstants.NAME )
    public String getAlias() {
        return this.alias;
    }

    @JsonProperty( SerializationConstants.SRC )
    public String getSrcAlias() {
        return this.srcAlias;
    }

    @JsonProperty( SerializationConstants.DST )
    public String getDstAlias() {
        return this.dstAlias;
    }

    @JsonProperty( SerializationConstants.KEY_FIELD )
    public List<FullQualifiedName> getKey() {
        return key;
    }

    @JsonProperty( SerializationConstants.PROPERTY_DEFINITIONS )
    public Map<FullQualifiedName, PropertyDefinition> getPropertyDefinitions() {
        return propertyDefinitions;
    }

    @JsonProperty( SerializationConstants.ENTITY_ID_GENERATOR )
    public Optional<SerializableFunction<Row, String>> getGenerator() {
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
        return "AssociationDefinition [entityTypeFqn=" + entityTypeFqn + ", entitySetName=" + entitySetName
                + ", srcAlias=" + srcAlias + ", dstAlias=" + dstAlias + ", propertyDefinitions=" + propertyDefinitions
                + ", key=" + key + ", alias=" + alias + ", generator=" + generator + "]";
    }

    @Override
    public int hashCode() {

        final int prime = 31;
        int result = 1;
        result = prime * result + ( ( alias == null ) ? 0 : alias.hashCode() );
        result = prime * result + ( ( dstAlias == null ) ? 0 : dstAlias.hashCode() );
        result = prime * result + ( ( entitySetName == null ) ? 0 : entitySetName.hashCode() );
        result = prime * result + ( ( entityTypeFqn == null ) ? 0 : entityTypeFqn.hashCode() );
        result = prime * result + ( ( generator == null ) ? 0 : generator.hashCode() );
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );
        result = prime * result + ( ( propertyDefinitions == null ) ? 0 : propertyDefinitions.hashCode() );
        result = prime * result + ( ( srcAlias == null ) ? 0 : srcAlias.hashCode() );
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
        AssociationDefinition other = (AssociationDefinition) obj;

        if ( alias == null ) {
            if ( other.alias != null )
                return false;
        } else if ( !alias.equals( other.alias ) )
            return false;

        if ( dstAlias == null ) {
            if ( other.dstAlias != null )
                return false;
        } else if ( !dstAlias.equals( other.dstAlias ) )
            return false;

        if ( entitySetName == null ) {
            if ( other.entitySetName != null )
                return false;
        } else if ( !entitySetName.equals( other.entitySetName ) )
            return false;

        if ( entityTypeFqn == null ) {
            if ( other.entityTypeFqn != null )
                return false;
        } else if ( !entityTypeFqn.equals( other.entityTypeFqn ) )
            return false;

        if ( generator == null ) {
            if ( other.generator != null )
                return false;
        } else if ( !generator.equals( other.generator ) )
            return false;

        if ( key == null ) {
            if ( other.key != null )
                return false;
        } else if ( !key.equals( other.key ) )
            return false;

        if ( propertyDefinitions == null ) {
            if ( other.propertyDefinitions != null )
                return false;
        } else if ( !propertyDefinitions.equals( other.propertyDefinitions ) )
            return false;

        if ( srcAlias == null ) {
            if ( other.srcAlias != null )
                return false;
        } else if ( !srcAlias.equals( other.srcAlias ) )
            return false;

        return true;
    }

    public static class Builder extends BaseBuilder<AssociationGroup.Builder, AssociationDefinition> {

        private FullQualifiedName                          entityTypeFqn;
        private String                                     entitySetName;
        private String                                     srcAlias;
        private String                                     dstAlias;
        private Map<FullQualifiedName, PropertyDefinition> propertyDefinitionMap;
        private SerializableFunction<Row, String>          generator;
        private List<FullQualifiedName>                    key;
        private String                                     alias;
        private Set<String>                                entityAliases;
        private boolean                                    useCurrentSync;

        public Builder(
                String alias,
                Set<String> entityAliases,
                AssociationGroup.Builder builder,
                BuilderCallback<AssociationDefinition> builderCallback ) {

            super( builder, builderCallback );

            this.alias = alias;
            this.propertyDefinitionMap = Maps.newHashMap();
            this.entityAliases = entityAliases;
            this.useCurrentSync = false;
        }

        public Builder key( FullQualifiedName... key ) {

            checkNotNull( key, "Key cannot be null." );
            checkArgument( ImmutableSet.copyOf( key ).size() == key.length, "Key must be a set of unique FQNs" );

            this.key = Arrays.asList( key );
            return this;
        }

        public Builder to( String entitySetName ) {

            this.entitySetName = entitySetName;
            return this;
        }

        public Builder ofType( FullQualifiedName entityTypeFqn ) {

            this.entityTypeFqn = entityTypeFqn;
            return this;
        }

        public Builder fromEntity( String srcAlias ) {

            checkArgument(
                    entityAliases.contains( srcAlias ),
                    "The source entity must be a previously defined alias."
            );

            this.srcAlias = srcAlias;
            return this;
        }

        public Builder toEntity( String dstAlias ) {

            checkArgument(
                    entityAliases.contains( dstAlias ),
                    "The destination entity must be a previously defined alias."
            );

            this.dstAlias = dstAlias;
            return this;
        }

        public Builder entityIdGenerator( SerializableFunction<Row, String> generator ) {

            this.generator = generator;
            return this;
        }

        public Builder useCurrentSync() {

            this.useCurrentSync = true;
            return this;
        }

        public PropertyDefinition.Builder<AssociationDefinition.Builder> addProperty(
                FullQualifiedName propertyTypeFqn ) {

            BuilderCallback<PropertyDefinition> onBuild = propertyDefinition -> {
                FullQualifiedName propertyDefFqn = propertyDefinition.getFullQualifiedName();
                if ( propertyDefinitionMap.containsKey( propertyDefFqn ) ) {
                    throw new IllegalStateException(
                            String.format( "encountered duplicate property: %s", propertyDefFqn )
                    );
                }
                propertyDefinitionMap.put( propertyDefFqn, propertyDefinition );
            };

            return new PropertyDefinition.Builder<AssociationDefinition.Builder>( propertyTypeFqn, this, onBuild );
        }

        public AssociationGroup.Builder ok() {

            if ( this.propertyDefinitionMap.size() == 0 ) {
                throw new IllegalStateException( "invoking addProperty() at least once is required" );
            }

            return super.ok( new AssociationDefinition( this ) );
        }
    }
}
