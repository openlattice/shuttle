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

import com.dataloom.LoomUtil;
import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.ApiFactoryFactory;
import com.dataloom.client.LoomClient;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.DataApi;
import com.dataloom.data.EntityKey;
import com.dataloom.data.requests.Association;
import com.dataloom.data.requests.BulkDataCreation;
import com.dataloom.data.requests.Entity;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.dataloom.sync.SyncApi;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.serialization.JacksonLambdaDeserializer;
import com.openlattice.shuttle.serialization.JacksonLambdaSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkNotNull;

public class Shuttle implements Serializable {

    private static final long serialVersionUID = -7356687761893337471L;

    private static final Logger logger = LoggerFactory
            .getLogger( Shuttle.class );

    private static transient LoadingCache<String, UUID>                             entitySetIdCache = null;
    private static transient LoadingCache<FullQualifiedName, UUID>                  propertyIdsCache = null;
    private static transient LoadingCache<UUID, UUID>                               ticketCache      = null;
    private static transient LoadingCache<String, LinkedHashSet<FullQualifiedName>> keyCache         = null;

    static {
        ObjectMappers.foreach( mapper -> {
            JacksonLambdaSerializer.registerWithMapper( mapper );
            FullQualifedNameJacksonSerializer.registerWithMapper( mapper );
            JacksonLambdaDeserializer.registerWithMapper( mapper );
            FullQualifedNameJacksonDeserializer.registerWithMapper( mapper );
        } );
    }

    private final LoomClient loomClient;

    public Shuttle( String authToken ) {
        // TODO: At some point we will have to handle mechanics of auth token expiration.
        this.loomClient = new LoomClient( () -> authToken );
    }

    public Shuttle( Environment environment, String authToken ) {
        // TODO: At some point we will have to handle mechanics of auth token expiration.
        this.loomClient = new LoomClient( environment, () -> authToken );
    }

    public Shuttle( ApiFactoryFactory apiFactorySupplier ) {
        this.loomClient = new LoomClient( apiFactorySupplier );
    }

    public void launchPayloadFlight( Map<Flight, Payload> flightsToPayloads ) throws InterruptedException {
        launch( flightsToPayloads.entrySet().stream()
                .collect( Collectors.toMap( entry -> entry.getKey(), entry -> entry.getValue().getPayload() ) ) );
    }

    public void launch( Map<Flight, Stream<Map<String, String>>> flightsToPayloads ) throws InterruptedException {

        EdmApi edmApi;
        SyncApi syncApi;
        PermissionsApi permissionsApi;

        try {
            edmApi = this.loomClient.getEdmApi();
            syncApi = this.loomClient.getSyncApi();
            permissionsApi = this.loomClient.getPermissionsApi();
        } catch ( ExecutionException e ) {
            logger.error( "Failed to retrieve apis." );
            return;
        }

        initializeEdmCaches( edmApi );

        Map<UUID, Boolean> entitySetIdsToSyncTypes = new HashMap<>();

        flightsToPayloads.keySet().forEach( flight -> {

            flight
                    .getEntities()
                    .forEach( entityDefinition -> {
                        UUID entitySetId = entitySetIdCache.getUnchecked( entityDefinition.getEntitySetName() );
                        assertPropertiesMatchEdm( entityDefinition.getEntitySetName(),
                                entitySetId,
                                entityDefinition.getProperties(),
                                edmApi );
                        entitySetIdsToSyncTypes.put(
                                entitySetId,
                                entityDefinition.useCurrentSync() );
                    } );

            flight
                    .getAssociations()
                    .forEach( associationDefinition -> {
                        UUID entitySetId = entitySetIdCache.getUnchecked( associationDefinition.getEntitySetName() );
                        assertPropertiesMatchEdm( associationDefinition.getEntitySetName(),
                                entitySetId,
                                associationDefinition.getProperties(),
                                edmApi );
                        entitySetIdsToSyncTypes.put(
                                entitySetId,
                                associationDefinition.useCurrentSync() );
                    } );
        } );

        Map<UUID, UUID> syncIds = new HashMap<>();

        entitySetIdsToSyncTypes.entrySet().forEach( entry -> {
            UUID entitySetId = entry.getKey();

            UUID syncId = ( entry.getValue() )
                    ? syncApi.getCurrentSyncId( entitySetId )
                    : syncApi.acquireSyncId( entitySetId );

            if ( syncId == null ) {
                logger.error( "Sync id was null for entity set id: {}", entitySetId );
                throw new NullPointerException( "Sync id was null for entity set id: " + entitySetId );
            }

            syncIds.put( entitySetId, syncId );
        } );

        flightsToPayloads.entrySet().forEach( entry -> {
            try {
                logger.info( "Launching flight: {}",
                        entry.getKey().getName() );
                launchFlight( entry.getKey(), entry.getValue(), syncIds );
                logger.info( "Finished flight: {}", entry.getKey().getName() );
            } catch ( InterruptedException e ) {
                logger.debug( "unable to launch flight" );
            }
        } );

        syncIds.entrySet().forEach( entry -> syncApi.setCurrentSyncId( entry.getKey(), entry.getValue() ) );

        DataApi dataApi;

        try {
            dataApi = this.loomClient.getDataApi();
        } catch ( ExecutionException e ) {
            logger.error( "Failed to get DataApi" );
            return;
        }

        ticketCache.asMap().keySet().stream().forEach( dataApi::releaseSyncTicket );

    }

    private void initializeEdmCaches( EdmApi edmApi ) {
        if ( entitySetIdCache == null ) {
            entitySetIdCache = CacheBuilder
                    .newBuilder()
                    .maximumSize( 1000 )
                    .build( new CacheLoader<String, UUID>() {
                        @Override
                        public UUID load( String entitySetName ) throws Exception {
                            return edmApi.getEntitySetId( entitySetName );
                        }
                    } );
        }

        if ( keyCache == null ) {
            keyCache = CacheBuilder
                    .newBuilder()
                    .maximumSize( 1000 )
                    .build( new CacheLoader<String, LinkedHashSet<FullQualifiedName>>() {
                        @Override
                        public LinkedHashSet<FullQualifiedName> load( String entitySetName ) throws Exception {
                            return edmApi.getEntityType(
                                    edmApi.getEntitySet( edmApi.getEntitySetId( entitySetName ) ).getEntityTypeId() )
                                    .getKey().stream()
                                    .map( propertyTypeId -> edmApi.getPropertyType( propertyTypeId ).getType() )
                                    .collect( Collectors.toCollection( LinkedHashSet::new ) );
                        }
                    } );
        }

        if ( propertyIdsCache == null ) {
            propertyIdsCache = CacheBuilder
                    .newBuilder()
                    .maximumSize( 1000 )
                    .build( new CacheLoader<FullQualifiedName, UUID>() {
                        @Override
                        public UUID load( FullQualifiedName propertyTypeFqn ) throws Exception {
                            return edmApi.getPropertyTypeId( propertyTypeFqn.getNamespace(),
                                    propertyTypeFqn.getName() );
                        }
                    } );
        }
    }

    private void assertPropertiesMatchEdm(
            String entitySetName,
            UUID entitySetId,
            Collection<PropertyDefinition> properties,
            EdmApi edmApi ) {

        Set<FullQualifiedName> propertyFqns = properties.stream()
                .map( propertyDef -> propertyDef.getFullQualifiedName() ).collect( Collectors.toSet() );
        Set<FullQualifiedName> entitySetPropertyFqns = edmApi.getEntityType(
                edmApi.getEntitySet( entitySetId ).getEntityTypeId() ).getProperties().stream()
                .map( id -> edmApi.getPropertyType( id ).getType() ).collect( Collectors.toSet() );
        Set<FullQualifiedName> illegalFqns = Sets.filter( propertyFqns, fqn -> !entitySetPropertyFqns.contains( fqn ) );
        if ( !illegalFqns.isEmpty() ) {
            illegalFqns.forEach( fqn -> {
                logger.error( "Entity set {} does not contain any property type with FQN: {}",
                        entitySetName,
                        fqn.toString() );
            } );
            throw new NullPointerException( "Illegal property types defined for entity set " + entitySetName );
        }
    }

    public void launchFlight( Flight flight, Stream<Map<String, String>> payload, Map<UUID, UUID> syncIds )
            throws InterruptedException {
        Optional<BulkDataCreation> remaining = payload
                .map( row -> {
                    DataApi dataApi;
                    EdmApi edmApi;

                    try {
                        dataApi = this.loomClient.getDataApi();
                        edmApi = this.loomClient.getEdmApi();
                    } catch ( ExecutionException e ) {
                        logger.error( "Failed to retrieve apis." );
                        throw new IllegalStateException( "Unable to retrieve APIs for execution" );
                    }

                    initializeEdmCaches( edmApi );

                    if ( ticketCache == null ) {
                        ticketCache = CacheBuilder
                                .newBuilder()
                                .maximumSize( 1000 )
                                .build( new CacheLoader<UUID, UUID>() {
                                    @Override
                                    public UUID load( UUID entitySetId ) throws Exception {
                                        final UUID syncId = syncIds.get( entitySetId );
                                        if ( syncId == null ) {
                                            logger.error( "Sync id for entity set id {} is null.", entitySetId );
                                            throw new NullPointerException( "Sync id is null." );
                                        }
                                        return dataApi.acquireSyncTicket( entitySetId, syncId );
                                    }
                                } );
                    }

                    Map<String, EntityKey> aliasesToEntityKey = new HashMap<>();
                    Set<UUID> syncTickets = Sets.newHashSet();
                    Set<Entity> entities = Sets.newHashSet();
                    Set<Association> associations = Sets.newHashSet();
                    Map<String, Boolean> wasCreated = new HashMap<>();

                    for ( EntityDefinition entityDefinition : flight.getEntities() ) {

                        UUID entitySetId = entitySetIdCache.getUnchecked( entityDefinition.getEntitySetName() );
                        UUID syncId = syncIds.get( entitySetId );
                        UUID ticket = ticketCache.getUnchecked( entitySetId );
                        syncTickets.add( ticket );
                        SetMultimap<UUID, Object> properties = HashMultimap.create();

                        for ( PropertyDefinition propertyDefinition : entityDefinition.getProperties() ) {
                            Object propertyValue = propertyDefinition.getPropertyValue().apply( row );
                            if ( propertyValue != null ) {
                                UUID propertyId = propertyIdsCache
                                        .getUnchecked( propertyDefinition.getFullQualifiedName() );
                                if ( propertyValue instanceof Iterable ) {
                                    properties.putAll( propertyId, (Iterable) propertyValue );
                                } else {
                                    properties.put(
                                            propertyId,
                                            propertyValue );
                                }
                            }
                        }

                /*
                 * For entityId generation to work correctly it is very important that Stream remain ordered. Ordered !=
                 * sequential vs parallel.
                 */

                        String entityId = ( entityDefinition.getGenerator().isPresent() )
                                ? entityDefinition.getGenerator().get().apply( row )
                                : generateDefaultEntityId( keyCache.getUnchecked( entityDefinition.getEntitySetName() ),
                                properties );

                        if ( StringUtils.isNotBlank( entityId ) ) {
                            EntityKey key = new EntityKey( entitySetId, entityId, syncId );
                            aliasesToEntityKey.put( entityDefinition.getAlias(), key );
                            entities.add( new Entity( key, properties ) );
                            wasCreated.put( entityDefinition.getAlias(), true );
                        } else {
                            wasCreated.put( entityDefinition.getAlias(), false );
                        }

                        MissionControl.signal();
                    }

                    for ( AssociationDefinition associationDefinition : flight.getAssociations() ) {

                        if ( wasCreated.get( associationDefinition.getSrcAlias() )
                                && wasCreated.get( associationDefinition.getDstAlias() ) ) {

                            UUID entitySetId = entitySetIdCache
                                    .getUnchecked( associationDefinition.getEntitySetName() );
                            UUID syncId = syncIds.get( entitySetId );
                            UUID ticket = ticketCache.getUnchecked( entitySetId );
                            syncTickets.add( ticket );
                            SetMultimap<UUID, Object> properties = HashMultimap.create();

                            for ( PropertyDefinition propertyDefinition : associationDefinition.getProperties() ) {
                                Object propertyValue = propertyDefinition.getPropertyValue().apply( row );
                                if ( propertyValue != null ) {
                                    properties.put(
                                            propertyIdsCache.getUnchecked( propertyDefinition.getFullQualifiedName() ),
                                            propertyValue );
                                }
                            }

                            String entityId = ( associationDefinition.getGenerator().isPresent() )
                                    ? associationDefinition.getGenerator().get().apply( row )
                                    : generateDefaultEntityId(
                                    keyCache.getUnchecked( associationDefinition.getEntitySetName() ),
                                    properties );

                            if ( StringUtils.isNotBlank( entityId ) ) {
                                EntityKey key = new EntityKey( entitySetId, entityId, syncId );
                                EntityKey src = aliasesToEntityKey.get( associationDefinition.getSrcAlias() );
                                EntityKey dst = aliasesToEntityKey.get( associationDefinition.getDstAlias() );
                                associations.add( new Association( key, src, dst, properties ) );
                            }
                        }

                        MissionControl.signal();
                    }

                    return new BulkDataCreation( syncTickets, entities, associations );
                } )
                .reduce( ( BulkDataCreation a, BulkDataCreation b ) -> {

                    DataApi dataApi;
                    try {
                        dataApi = this.loomClient.getDataApi();
                    } catch ( ExecutionException e ) {
                        logger.error( "Failed to retrieve apis." );
                        throw new IllegalStateException( "Unable to retrieve APIs for execution" );
                    }

                    a.getTickets().addAll( b.getTickets() );
                    a.getAssociations().addAll( b.getAssociations() );
                    a.getEntities().addAll( b.getEntities() );

                    if ( a.getAssociations().size() > 10000 || a.getEntities().size() > 10000 ) {
                        dataApi.createEntityAndAssociationData( a );// );
                        return new BulkDataCreation( new HashSet<>(), new HashSet<>(), new HashSet<>() );
                    }

                    return a;
                } );

        remaining.ifPresent( r -> {
            DataApi dataApi;
            try {
                dataApi = this.loomClient.getDataApi();
                dataApi.createEntityAndAssociationData( r );
            } catch ( ExecutionException e ) {
                logger.error( "Failed to retrieve apis." );
                throw new IllegalStateException( "Unable to retrieve APIs for execution" );
            }
        } );
    }

    /*
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     */
    private static String generateDefaultEntityId(
            LinkedHashSet<FullQualifiedName> key,
            SetMultimap<UUID, Object> properties ) {

        return LoomUtil.generateDefaultEntityId(
                checkNotNull( key, "Key properties must be configured for entity id generation." )
                        .stream()
                        .map( propertyIdsCache::getUnchecked ),
                properties );
    }
}
