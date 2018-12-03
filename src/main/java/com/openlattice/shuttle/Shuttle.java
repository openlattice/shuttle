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

import static com.google.common.base.Preconditions.checkNotNull;

import com.dataloom.mappers.ObjectMappers;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.openlattice.ApiUtil;
import com.openlattice.client.ApiClient;
import com.openlattice.client.ApiFactoryFactory;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.data.*;
import com.openlattice.data.integration.*;
import com.openlattice.data.integration.Entity;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.data.storage.PostgresDataHasher;
import com.openlattice.edm.EdmApi;
import com.openlattice.graph.edge.EdgeKey;
import com.openlattice.retrofit.RhizomeByteConverterFactory;
import com.openlattice.retrofit.RhizomeCallAdapterFactory;
import com.openlattice.retrofit.RhizomeJacksonConverterFactory;
import com.openlattice.rhizome.proxy.RetrofitBuilders;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.serialization.JacksonLambdaDeserializer;
import com.openlattice.shuttle.serialization.JacksonLambdaSerializer;

import java.io.Serializable;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.openlattice.data.integration.StorageDestination;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

public class Shuttle implements Serializable {
    private static final long                                    serialVersionUID      = -7356687761893337471L;
    private static       ConcurrentMap<UUID, StorageDestination> propertyToStorageDest = new ConcurrentHashMap<>();

    private static final Logger logger = LoggerFactory
            .getLogger( Shuttle.class );

    private static final     int                                                    UPLOAD_BATCH_SIZE = 100000;
    private static transient LoadingCache<String, UUID>                             entitySetIdCache  = null;
    private static transient LoadingCache<FullQualifiedName, UUID>                  propertyIdsCache  = null;
    private static transient LoadingCache<String, LinkedHashSet<FullQualifiedName>> keyCache          = null;

    static {
        ObjectMappers.foreach( mapper -> {
            JacksonLambdaSerializer.registerWithMapper( mapper );
            FullQualifiedNameJacksonSerializer.registerWithMapper( mapper );
            JacksonLambdaDeserializer.registerWithMapper( mapper );
        } );
    }

    private final ApiClient apiClient;

    public Shuttle( String authToken ) {
        // TODO: At some point we will have to handle mechanics of auth token expiration.
        this.apiClient = new ApiClient( () -> authToken );
    }

    public Shuttle( Environment environment, String authToken ) {
        // TODO: At some point we will have to handle mechanics of auth token expiration.
        this.apiClient = new ApiClient( environment, () -> authToken );
    }

    public Shuttle( ApiFactoryFactory apiFactorySupplier ) {
        this.apiClient = new ApiClient( apiFactorySupplier );
    }

    public void launchPayloadFlight( Map<Flight, Payload> flightsToPayloads ) throws InterruptedException {
        launch( flightsToPayloads.entrySet().stream()
                .collect( Collectors.toMap( entry -> entry.getKey(), entry -> entry.getValue().getPayload() ) ) );
    }

    public void launch( Map<Flight, Stream<Map<String, String>>> flightsToPayloads ) throws InterruptedException {

        EdmApi edmApi;

        try {
            edmApi = this.apiClient.getEdmApi();
        } catch ( ExecutionException e ) {
            logger.error( "Failed to retrieve apis." );
            return;
        }

        initializeEdmCaches( edmApi );

        flightsToPayloads.keySet().forEach( flight -> {

            flight
                    .getEntities()
                    .forEach( entityDefinition -> {
                        UUID entitySetId = entitySetIdCache.getUnchecked( entityDefinition.getEntitySetName() );
                        assertPropertiesMatchEdm( entityDefinition.getEntitySetName(),
                                entitySetId,
                                entityDefinition.getProperties(),
                                edmApi );
                    } );

            flight
                    .getAssociations()
                    .forEach( associationDefinition -> {
                        UUID entitySetId = entitySetIdCache.getUnchecked( associationDefinition.getEntitySetName() );
                        assertPropertiesMatchEdm( associationDefinition.getEntitySetName(),
                                entitySetId,
                                associationDefinition.getProperties(),
                                edmApi );
                    } );
        } );

        flightsToPayloads.entrySet().forEach( entry -> {
            logger.info( "Launching flight: {}", entry.getKey().getName() );
            launchFlight( entry.getKey(), entry.getValue() );
            logger.info( "Finished flight: {}", entry.getKey().getName() );
        } );

        System.exit( 0 );
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

    private Set<Object> addAndReturn( Set<Object> s, Object o ) {
        s.add( o );
        return s;
    }

    public void launchFlight( Flight flight, Stream<Map<String, String>> payload ) {
        Optional<BulkDataCreation> remaining = payload
                .parallel()
                .map( row -> {
                    EdmApi edmApi;

                    try {
                        edmApi = this.apiClient.getEdmApi();
                    } catch ( ExecutionException e ) {
                        logger.error( "Failed to retrieve apis." );
                        throw new IllegalStateException( "Unable to retrieve APIs for execution" );
                    }

                    initializeEdmCaches( edmApi );

                    Map<String, EntityKey> aliasesToEntityKey = new HashMap<>();
                    Set<Entity> entities = Sets.newHashSet();
                    Set<Association> associations = Sets.newHashSet();
                    Map<String, Boolean> wasCreated = new HashMap<>();

                    if ( flight.condition.isPresent() ) {
                        Object out = flight.valueMapper.apply( row );
                        if ( !( (Boolean) out ).booleanValue() ) {
                            return new BulkDataCreation( entities, associations );
                        }
                    }

                    for ( EntityDefinition entityDefinition : flight.getEntities() ) {

                        Boolean condition = true;
                        if ( entityDefinition.condition.isPresent() ) {
                            Object out = entityDefinition.valueMapper.apply( row );
                            if ( !( (Boolean) out ).booleanValue() ) {
                                condition = false;
                            }
                        }

                        UUID entitySetId = entitySetIdCache.getUnchecked( entityDefinition.getEntitySetName() );
                        Map<UUID, Set<Object>> properties = new HashMap<>();

                        for ( PropertyDefinition propertyDefinition : entityDefinition.getProperties() ) {
                            StorageDestination storageDest;
                            Object propertyValue = propertyDefinition.getPropertyValue().apply( row );
                            if ( propertyValue != null ) {
                                String stringProp = propertyValue.toString();
                                if ( !StringUtils.isBlank( stringProp ) ) {
                                    UUID propertyId = propertyIdsCache
                                            .getUnchecked( propertyDefinition.getFullQualifiedName() );
                                    storageDest = propertyDefinition.getStorageDest();
                                    propertyToStorageDest.put( propertyId, storageDest );
                                    if ( propertyValue instanceof Iterable ) {
                                        properties
                                                .putAll( ImmutableMap.of( propertyId,
                                                        Sets.newHashSet( (Iterable<?>) propertyValue ) ) );
                                    } else {
                                        properties.compute( propertyId,
                                                ( k, v ) -> ( v == null )
                                                        ? addAndReturn( new HashSet<>(), propertyValue )
                                                        : addAndReturn( v, propertyValue ) );
                                    }
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

                        if ( StringUtils.isNotBlank( entityId ) & condition & properties.size() > 0 ) {
                            EntityKey key = new EntityKey( entitySetId, entityId );
                            aliasesToEntityKey.put( entityDefinition.getAlias(), key );
                            entities.add( new Entity( key, properties ) );
                            wasCreated.put( entityDefinition.getAlias(), true );
                        } else {
                            wasCreated.put( entityDefinition.getAlias(), false );
                        }

                        MissionControl.signal();
                    }

                    for ( AssociationDefinition associationDefinition : flight.getAssociations() ) {

                        if ( associationDefinition.condition.isPresent() ) {
                            Object out = associationDefinition.valueMapper.apply( row );
                            if ( !( (Boolean) out ).booleanValue() ) {
                                continue;
                            }
                        }

                        if ( !wasCreated.containsKey( associationDefinition.getDstAlias() ) ) {
                            logger.error( "Destination " + associationDefinition.getDstAlias()
                                    + " cannot be found to construct association " + associationDefinition.getAlias() );
                        }

                        if ( !wasCreated.containsKey( associationDefinition.getSrcAlias() ) ) {
                            logger.error( "Source " + associationDefinition.getSrcAlias()
                                    + " cannot be found to construct association " + associationDefinition.getAlias() );
                        }
                        if ( wasCreated.get( associationDefinition.getSrcAlias() )
                                && wasCreated.get( associationDefinition.getDstAlias() ) ) {

                            UUID entitySetId = entitySetIdCache
                                    .getUnchecked( associationDefinition.getEntitySetName() );
                            Map<UUID, Set<Object>> properties = new HashMap<>();

                            for ( PropertyDefinition propertyDefinition : associationDefinition.getProperties() ) {
                                Object propertyValue = propertyDefinition.getPropertyValue().apply( row );
                                if ( propertyValue != null ) {
                                    var propertyId = propertyIdsCache
                                            .getUnchecked( propertyDefinition.getFullQualifiedName() );
                                    StorageDestination storageDest = propertyDefinition.getStorageDest();
                                    propertyToStorageDest.put( propertyId, storageDest );
                                    if ( propertyValue instanceof Iterable ) {
                                        properties
                                                .putAll( ImmutableMap
                                                        .of( propertyId,
                                                                Sets.newHashSet( (Iterable<?>) propertyValue ) ) );
                                    } else {
                                        properties.compute( propertyId,
                                                ( k, v ) -> ( v == null )
                                                        ? addAndReturn( new HashSet<>(), propertyValue )
                                                        : addAndReturn( v, propertyValue ) );
                                    }
                                }
                            }

                            String entityId = ( associationDefinition.getGenerator().isPresent() )
                                    ? associationDefinition.getGenerator().get().apply( row )
                                    : generateDefaultEntityId(
                                    keyCache.getUnchecked( associationDefinition.getEntitySetName() ),
                                    properties );

                            if ( StringUtils.isNotBlank( entityId ) ) {
                                EntityKey key = new EntityKey( entitySetId, entityId );
                                EntityKey src = aliasesToEntityKey.get( associationDefinition.getSrcAlias() );
                                EntityKey dst = aliasesToEntityKey.get( associationDefinition.getDstAlias() );
                                associations.add( new Association( key, src, dst, properties ) );
                            }
                        }

                        MissionControl.signal();
                    }

                    return new BulkDataCreation( entities,
                            associations );
                } )
                .reduce( ( BulkDataCreation a, BulkDataCreation b ) -> {

                    DataIntegrationApi dataApi;
                    dataApi = this.apiClient.getDataIntegrationApi();

                    a.getAssociations().addAll( b.getAssociations() );
                    a.getEntities().addAll( b.getEntities() );

                    if ( a.getAssociations().size() > 100000 || a.getEntities().size() > 100000 ) {
                        Set<EntityKey> entityKeys = a.getEntities().stream().map( entity -> entity.getKey() )
                                .collect( Collectors.toSet() );
                        entityKeys.addAll( a.getAssociations().stream().map( association -> association.getKey() ).collect(
                                Collectors.toSet() ) );
                        Map<UUID, Map<String, UUID>> bulkEntitySetIds = dataApi.getEntityKeyIds( entityKeys );
                        sendDataToDataSink( a, bulkEntitySetIds, dataApi );
                        return new BulkDataCreation( new HashSet<>(),
                                new HashSet<>() );
                    }

                    return a;
                } );

        remaining.ifPresent( r -> {
            DataIntegrationApi dataApi;
            dataApi = this.apiClient.getDataIntegrationApi();
            Set<EntityKey> entityKeys = r.getEntities().stream().map( entity -> entity.getKey() )
                    .collect( Collectors.toSet() );
            entityKeys.addAll( r.getAssociations().stream().map( association -> association.getKey() ).collect(
                    Collectors.toSet() ) );
            Map<UUID, Map<String, UUID>> bulkEntitySetIds = dataApi.getEntityKeyIds( entityKeys );
            sendDataToDataSink( r, bulkEntitySetIds, dataApi );
        } );
    }

    /*
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     */
    private static String generateDefaultEntityId(
            LinkedHashSet<FullQualifiedName> key,
            Map<UUID, Set<Object>> properties ) {

        return ApiUtil.generateDefaultEntityId(
                checkNotNull( key, "Key properties must be configured for entity id generation." )
                        .stream()
                        .map( propertyIdsCache::getUnchecked ),
                properties );
    }

    private static void sendDataToDataSink(
            BulkDataCreation a,
            Map<UUID, Map<String, UUID>> bulkEntitySetIds,
            DataIntegrationApi dataApi ) {
        //create data structures to store data for s3 and postgres data sinks
        Set<S3EntityData> s3Entities = Sets.newHashSet();
        Set<EntityData> postgresEntities = Sets.newHashSet();

        //sort entities by storage dest
        a.getEntities().forEach( entity -> {
            Map<UUID, Set<Object>> postgresProperties = new HashMap<>();
            UUID entitySetId = entity.getEntitySetId();
            String entityId = entity.getEntityId();
            UUID entityKeyId = bulkEntitySetIds.get( entitySetId ).get( entityId );

            //for each set of properties in the entity
            entity.getDetails().entrySet().forEach( e -> {
                if ( propertyToStorageDest.get( e.getKey() ).equals( StorageDestination.AWS ) ) {
                    for ( Object o : e.getValue() ) {
                        String encodedProperty = (String) o;
                        byte[] property = Base64.getEncoder().encode( encodedProperty.getBytes() );
                        String propertyHash = PostgresDataHasher
                                .hashObjectToHex( property, EdmPrimitiveTypeKind.Binary );
                        s3Entities.add( new S3EntityData( entitySetId,
                                entityKeyId,
                                e.getKey(),
                                propertyHash,
                                property ) );
                    }
                } else {
                    postgresProperties.put( e.getKey(), e.getValue() );
                }
            } );
            if ( !postgresProperties.isEmpty() ) {
                postgresEntities.add( new EntityData( entitySetId, entityId, entityKeyId, postgresProperties ) );
            }
        } );

        //write associations to postgres
        Set<Association> associations= a.getAssociations();
        Set<DataEdgeKey> edges = associations.stream().map( association -> {
            UUID srcEntitySetId = association.getSrc().getEntitySetId();
            String srcEntityId = association.getSrc().getEntityId();
            UUID srcEntityKeyId = bulkEntitySetIds.get( srcEntitySetId ).get( srcEntityId );

            UUID dstEntitySetId = association.getDst().getEntitySetId();
            String dstEntityId = association.getDst().getEntityId();
            UUID dstEntityKeyId = bulkEntitySetIds.get( dstEntitySetId ).get( dstEntityId );

            UUID keyEntitySetId = association.getKey().getEntitySetId();
            String keyEntityId = association.getKey().getEntityId();
            UUID keyEntityKeyId = bulkEntitySetIds.get( keyEntitySetId ).get( keyEntityId );

            postgresEntities.add(new EntityData(keyEntitySetId, keyEntityId, keyEntityKeyId, association.getDetails()));

            return new DataEdgeKey( new EntityDataKey( srcEntitySetId, srcEntityKeyId ),
                    new EntityDataKey( dstEntitySetId, dstEntityKeyId ),
                    new EntityDataKey( keyEntitySetId, keyEntityKeyId ) );

        } ).collect( Collectors.toSet());

        dataApi.createEdges( edges );
        dataApi.sinkToPostgres( postgresEntities );

        Map<URL, byte[]> urls = dataApi.generatePresignedUrls( s3Entities );

        OkHttpClient client = RetrofitBuilders.okHttpClient().build();

        Retrofit retrofit = new Retrofit.Builder()
                .baseUrl( "https://tempy-media-storage.s3.us-gov-west-1.amazonaws.com/" )
                .addConverterFactory( new RhizomeByteConverterFactory() )
                .addConverterFactory( new RhizomeJacksonConverterFactory( ObjectMappers.getJsonMapper() ) )
                .addCallAdapterFactory( new RhizomeCallAdapterFactory() )
                .client( client )
                .build();
        S3Api s3Api = retrofit.create( S3Api.class );
        urls.forEach( ( k, v ) -> {
            RequestBody body = RequestBody.create( MediaType.parse( "application/octet-stream" ), v );
            s3Api.writeToS3( k.toString(), body );
            System.out.println( k );
            System.out.println( k.toString() );
            System.out.println( "*************************************" );
        } );

    }

}
