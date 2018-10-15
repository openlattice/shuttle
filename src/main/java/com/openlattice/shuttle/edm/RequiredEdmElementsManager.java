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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.*;
import com.openlattice.authorization.Ace;
import com.openlattice.authorization.Acl;
import com.openlattice.authorization.AclData;
import com.openlattice.authorization.Action;
import com.openlattice.authorization.Permission;
import com.openlattice.authorization.PermissionsApi;
import com.openlattice.authorization.Principal;
import com.openlattice.authorization.PrincipalType;
import com.openlattice.authorization.securable.SecurableObjectType;
import com.openlattice.edm.EdmApi;
import com.openlattice.edm.EntitySet;
import com.openlattice.edm.type.AssociationType;
import com.openlattice.edm.type.EntityType;
import com.openlattice.edm.type.PropertyType;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.security.InvalidParameterException;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RequiredEdmElementsManager {

    private static final Logger         logger = LoggerFactory.getLogger( RequiredEdmElementsManager.class );
    private final        EdmApi         edmApi;
    private final        PermissionsApi permissionsApi;

    public RequiredEdmElementsManager( EdmApi edmApi, PermissionsApi permissionsApi ) {

        this.edmApi = edmApi;
        this.permissionsApi = permissionsApi;
    }

    public void ensureEdmElementsExist( RequiredEdmElements elements ) {

        Map<FullQualifiedName, UUID> propertyIds = elements
                .getPropertyTypes()
                .stream()
                .flatMap( rp -> rp.getProperties().stream().map( bind( rp ) ) )
                .collect( Collectors.toMap( PropertyType::getType, this::getOrCreateType ) );

        Map<FullQualifiedName, UUID> entityTypeIds = elements
                .getEntityTypes()
                .stream()
                .collect( Collectors.toMap(
                        EntityTypeModel::getType,
                        e -> getOrCreateEntityType( e, propertyIds )
                ) );

        Map<FullQualifiedName, UUID> assocTypeIds = elements
                .getAssociationTypes()
                .stream()
                .collect( Collectors.toMap(
                        e -> e.getType().getType(),
                        e -> getOrCreateAssociationType( e, propertyIds, entityTypeIds )
                ) );

        // catch silent failures due to overlapping types
        Preconditions.checkState(
                Sets.intersection( entityTypeIds.keySet(), assocTypeIds.keySet() ).isEmpty(),
                "Entity type ids cannot overlap with association type ids"
        );

        entityTypeIds.putAll( assocTypeIds );

        Set<UUID> entitySetIds = elements
                .getEntitySets()
                .stream()
                .map( e -> getOrCreateEntitySet( e, entityTypeIds ) )
                .collect( Collectors.toSet() );
    }

    public UUID getOrCreateAssociationType(
            AssociationTypeModel assocType,
            Map<FullQualifiedName, UUID> propertyIds,
            Map<FullQualifiedName, UUID> entityTypeIds ) {

        final FullQualifiedName fqn = assocType.getType().getType();
        UUID entityTypeId = edmApi.getEntityTypeId( fqn.getNamespace(), fqn.getName() );

        if ( entityTypeId == null ) {

            EntityTypeModel model = assocType.getType();
            LinkedHashSet<UUID> src = new LinkedHashSet<>( assocType.getSrc().size() );
            LinkedHashSet<UUID> dst = new LinkedHashSet<>( assocType.getDst().size() );

            assocType
                    .getSrc()
                    .stream()
                    .map( entityTypeIds::get )
                    .forEachOrdered( src::add );

            assocType
                    .getDst()
                    .stream()
                    .map( entityTypeIds::get )
                    .forEachOrdered( dst::add );

            EntityType entityType = fromModel(
                    edmApi,
                    model,
                    propertyIds,
                    SecurableObjectType.AssociationType
            );

            AssociationType toBeCreated = new AssociationType(
                    Optional.of( entityType ),
                    src,
                    dst,
                    assocType.isBidirectional()
            );

            entityTypeId = edmApi.createAssociationType( toBeCreated );
        } else {
            ensureAssociationSetsAreEqual( assocType, edmApi.getAssociationTypeById( entityTypeId ) );
        }

        return Preconditions.checkNotNull( entityTypeId, "Entity type id cannot be null." );
    }

    public UUID getOrCreateEntitySet( EntitySetModel entitySet, Map<FullQualifiedName, UUID> entityTypeIds ) {

        final FullQualifiedName fqn = entitySet.getType();
        UUID entitySetId = edmApi.getEntitySetId( entitySet.getName() );

        if ( entitySetId == null ) {

            EntitySet toBeCreated = new EntitySet(
                    entityTypeIds.get( entitySet.getType() ),
                    entitySet.getName(),
                    entitySet.getTitle(),
                    Optional.of( entitySet.getDescription() ),
                    entitySet.getContacts()
            );

            entitySetId = Iterables.getOnlyElement(
                    edmApi.createEntitySets( ImmutableSet.of( toBeCreated ) ).values()
            );
        } else {
            ensureEntitySetAreEqual( entitySet, edmApi.getEntitySet( entitySetId ) );
        }

        final UUID esId = entitySetId;

        for ( String owner : entitySet.getOwners() ) {
            Set<UUID> ownableAclIds = edmApi.getEntityType( entityTypeIds.get( entitySet.getType() ) )
                    .getProperties();

            Set<List<UUID>> aclKeys = ownableAclIds.stream()
                    .map( idPart -> ImmutableList.of( esId, idPart ) )
                    .collect( Collectors.toSet() );
            aclKeys.add( ImmutableList.of( esId ) );

            for ( List<UUID> aclKey : aclKeys ) {
                addOwnerIfNotPresent( aclKey, owner );
            }
        }

        return checkNotNull( entitySetId, "Entity set doesn't exist or failed to create entity set" );
    }

    public UUID getOrCreateType( PropertyType propertyType ) {

        final FullQualifiedName fqn = propertyType.getType();
        UUID propertyTypeId = edmApi.getPropertyTypeId( fqn.getNamespace(), fqn.getName() );

        if ( propertyTypeId == null ) {
            propertyTypeId = edmApi.createPropertyType( propertyType );
        } else {
            ensurePropertyTypesAreEqual( propertyType, edmApi.getPropertyType( propertyTypeId ) );
        }

        return checkNotNull( propertyTypeId,
                "Property type " + fqn.getFullQualifiedNameAsString()
                        + " doesn't exist or failed to create property type" );
    }

    public UUID getOrCreateEntityType( EntityTypeModel entityTypeModel, Map<FullQualifiedName, UUID> propertyIds ) {

        final FullQualifiedName fqn = entityTypeModel.getType();
        UUID entityTypeId = edmApi.getEntityTypeId( fqn.getNamespace(), fqn.getName() );

        if ( entityTypeId == null ) {
            EntityType toBeCreated = fromModel( edmApi, entityTypeModel, propertyIds );
            entityTypeId = edmApi.createEntityType( toBeCreated );
        } else {
            ensureEntityTypesAreEqual( entityTypeModel, edmApi.getEntityType( entityTypeId ), propertyIds );
        }

        return checkNotNull( entityTypeId, "Entity type doesn't exist or failed to create entity type" );
    }

    private void addOwnerIfNotPresent( List<UUID> aclKey, String owner ) {
        Acl acl = new Acl( aclKey,
                ImmutableList
                        .of( new Ace( new Principal( PrincipalType.USER, owner ),
                                EnumSet.allOf( Permission.class ) ) ) );
        permissionsApi.updateAcl( new AclData( acl, Action.ADD ) );
    }

    private void ensurePropertyTypesAreEqual( PropertyType a, PropertyType b ) {
        // TODO: implement equality check, ids will be different
    }

    private void ensureEntityTypesAreEqual(
            EntityTypeModel a,
            EntityType b,
            Map<FullQualifiedName, UUID> propertyTypes ) {

        Sets
                .difference(
                        a.getProperties().stream().map( propertyTypes::get ).collect( Collectors.toSet() ),
                        b.getProperties()
                )
                .stream().filter( Objects::nonNull )
                .forEach( propId -> edmApi.addPropertyTypeToEntityType( b.getId(), propId ) );
    }

    private void ensureEntitySetAreEqual( EntitySetModel a, EntitySet b ) {
        // TODO: implement equality check, ids will be different
    }

    private static void ensureAssociationSetsAreEqual(
            AssociationTypeModel assocType,
            AssociationType associationTypeById ) {
        // TODO: implement equality check
    }

    public static EntityType fromModel(
            EdmApi edmApi,
            EntityTypeModel model,
            Map<FullQualifiedName, UUID> propertyIds ) {

        return fromModel( edmApi, model, propertyIds, SecurableObjectType.EntityType );
    }

    public static EntityType fromModel(
            EdmApi edmApi,
            EntityTypeModel model,
            Map<FullQualifiedName, UUID> propertyIds,
            SecurableObjectType objectType ) {

        LinkedHashSet<UUID> key = new LinkedHashSet<>( model.getKey().size() );
        LinkedHashSet<UUID> properties = new LinkedHashSet<>( model.getProperties().size() );

        model
                .getKey()
                .stream()
                .map( fqn -> {
                    UUID propertyId = propertyIds.get( fqn );
                    if ( propertyId == null ) {
                        logger.error( "Unable to resolve fqn {} to id.", fqn );
                        throw new InvalidParameterException( "Invalid fqn: " + fqn.toString() );
                    }
                    return propertyId;
                } )
                .forEachOrdered( key::add );

        model
                .getProperties()
                .stream()
                .map( fqn -> {
                    UUID propertyId = propertyIds.get( fqn );
                    if ( propertyId == null ) {
                        logger.error( "Unable to resolve fqn {} to id.", fqn );
                        throw new InvalidParameterException( "Invalid fqn: " + fqn.toString() );
                    }
                    return propertyId;
                } )
                .forEachOrdered( properties::add );

        return new EntityType(
                model.getType(),
                model.getTitle(),
                model.getDescription(),
                ImmutableSet.of(),
                key,
                properties,
                LinkedHashMultimap.create(),
                model.getBaseType().map( bt -> edmApi.getEntityTypeId( bt.getNamespace(), bt.getName() ) ),
                Optional.of( objectType )
        );
    }

    public static Function<PropertyMetadata, PropertyType> bind( RequiredProperties requiredProperties ) {

        return pm -> new PropertyType(
                new FullQualifiedName( pm.getType() ),
                pm.getTitle(),
                pm.getDescription(),
                requiredProperties.getSchemas(),
                requiredProperties.getDatatype()
        );
    }
}
