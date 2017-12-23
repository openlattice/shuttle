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

package com.openlattice.shuttle.test;

import static com.openlattice.shuttle.util.CsvUtil.newDefaultMapper;
import static com.openlattice.shuttle.util.CsvUtil.newDefaultSchemaFromHeader;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.ApiFactory;
import com.dataloom.client.ApiFactoryFactory;
import com.dataloom.data.DataApi;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mapstores.TestDataFactory;
import com.dataloom.streams.StreamUtil;
import com.dataloom.sync.SyncApi;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class ShuttleTest extends ShuttleTestBootstrap {
    private static final Logger logger = LoggerFactory.getLogger( ShuttleTest.class );
    private static Stream<Map<String, String>> payload;

    private static String CYPHERS_ALIAS      = "cyphers";
    private static String MORE_CYPHERS_ALIAS = "moreCyphers";
    private static String ASSOCIATION_ALIAS  = "cypherToCypher";

    private static UUID CYPHER_ES_SYNC_ID       = UUIDs.timeBased();
    private static UUID MORE_CYPHERS_ES_SYNC_ID = UUIDs.timeBased();
    private static UUID ASSOCIATION_ES_SYNC_ID  = UUIDs.timeBased();

    private static PropertyType ALGO_PT        = TestDataFactory.propertyType();
    private static PropertyType MODE_PT        = TestDataFactory.propertyType();
    private static PropertyType KEY_SIZE_PT    = TestDataFactory.propertyType();
    private static PropertyType CYPHER_HASH_PT = TestDataFactory.propertyType();
    private static PropertyType ID_PT          = TestDataFactory.propertyType();

    private static Set<PropertyType> PTS = Sets.newHashSet( ALGO_PT,
            MODE_PT,
            KEY_SIZE_PT,
            CYPHER_HASH_PT,
            ID_PT );

    private static EntityType CYPHERS_ET = TestDataFactory
            .childEntityTypeWithPropertyType( null,
                    PTS.stream().map( pt -> pt.getId() ).collect( Collectors.toSet() ),
                    ALGO_PT );

    private static AssociationType ASSOCIATION_TYPE = TestDataFactory
            .associationTypeWithProperties( PTS.stream().map( pt -> pt.getId() ).collect( Collectors.toSet() ), ID_PT );
    private static EntityType      ASSOCIATION_ET   = ASSOCIATION_TYPE.getAssociationEntityType();

    private static EntitySet CYPHERS_ES      = TestDataFactory
            .entitySetWithType( CYPHERS_ET.getId() );
    private static EntitySet MORE_CYPHERS_ES = TestDataFactory
            .entitySetWithType( CYPHERS_ET.getId() );
    private static EntitySet ASSOCIATION_ES  = TestDataFactory
            .entitySetWithType( ASSOCIATION_ET.getId() );

    @Test
    public void testFlight() throws InterruptedException {

        ApiFactoryFactory apiFactorySupplier = () -> {

            EdmApi edmApi = mock( EdmApi.class );
            DataApi mockDataApi = mock( DataApi.class );
            SyncApi mockSyncApi = mock( SyncApi.class );
            PermissionsApi mockPermissionsApi = mock( PermissionsApi.class );
            ApiFactory mockApiFactory = mock( ApiFactory.class );

            when( mockApiFactory.create( DataApi.class ) )
                    .thenAnswer( Answers.incrementCreateDataApiCount( mockDataApi ) );

            when( mockApiFactory.create( EdmApi.class ) )
                    .thenReturn( edmApi );

            when( mockApiFactory.create( SyncApi.class ) )
                    .thenReturn( mockSyncApi );

            when( mockApiFactory.create( PermissionsApi.class ) )
                    .thenReturn( mockPermissionsApi );

            when( edmApi.getEntitySetId( CYPHERS_ES.getName() ) ).thenReturn( CYPHERS_ES.getId() );
            when( edmApi.getEntitySetId( MORE_CYPHERS_ES.getName() ) ).thenReturn( MORE_CYPHERS_ES.getId() );
            when( edmApi.getEntitySetId( ASSOCIATION_ES.getName() ) ).thenReturn( ASSOCIATION_ES.getId() );
            when( mockSyncApi.acquireSyncId( CYPHERS_ES.getId() ) ).thenReturn( CYPHER_ES_SYNC_ID );
            when( mockSyncApi.acquireSyncId( MORE_CYPHERS_ES.getId() ) ).thenReturn( MORE_CYPHERS_ES_SYNC_ID );
            when( mockSyncApi.acquireSyncId( ASSOCIATION_ES.getId() ) ).thenReturn( ASSOCIATION_ES_SYNC_ID );

            PTS.forEach( pt -> {
                when( edmApi.getPropertyTypeId( pt.getType().getNamespace(), pt.getType().getName() ) )
                        .thenReturn( pt.getId() );
                when( edmApi.getPropertyType( pt.getId() ) ).thenReturn( pt );
            } );

            when( edmApi.getEntityType( CYPHERS_ET.getId() ) ).thenReturn( CYPHERS_ET );
            when( edmApi.getEntityType( ASSOCIATION_ET.getId() ) ).thenReturn( ASSOCIATION_ET );
            when( edmApi.getEntitySet( CYPHERS_ES.getId() ) ).thenReturn( CYPHERS_ES );
            when( edmApi.getEntitySet( MORE_CYPHERS_ES.getId() ) ).thenReturn( MORE_CYPHERS_ES );
            when( edmApi.getEntitySet( ASSOCIATION_ES.getId() ) ).thenReturn( ASSOCIATION_ES );
            //
            // PROPERTIES.entrySet()
            // .forEach( e -> when( edmApi.getPropertyTypeId( e.getAclKey().getNamespace(), e.getAclKey().getName() ) )
            // .thenReturn( e.getValue() ) );

            doAnswer( Answers.incrementCreateDataInvocationCount() )
                    .when( mockDataApi ).createEntityAndAssociationData( Mockito.any() );

            when( mockDataApi.acquireSyncTicket( Mockito.any(), Mockito.any() ) ).thenReturn( new UUID( 1, 1 ) );
            // Mockito.verify( mockDataApi, Mockito.atLeastOnce() ).releaseSyncTicket( new UUID( 1, 1 ) );
            return mockApiFactory;
        };

        // @formatter:off
        Flight flight = getFlight();
        // @formatter:on

        Map<Flight, Stream<Map<String, String>>> flights = Maps.newHashMap();
        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( apiFactorySupplier );
        shuttle.launch( flights );

        Assert.assertEquals( 1, Answers.getCreateDataInvocationCount() );
        Assert.assertEquals( 2, Answers.getCreateDataApiInvocationCount() );
    }

    @Test(
            expected = IllegalStateException.class )
    public void testNoEntities() {

        Flight flight = Flight.newFlight().done();
    }

    @Test(
            expected = IllegalStateException.class )
    public void testNoProperties() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( CYPHERS_ALIAS).to( CYPHERS_ES.getName() ).endEntity()
                .endEntities().done();
        // @formatter:on
    }

    @Test(
            expected = IllegalStateException.class )
    public void testNoDuplicateEntities() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS)
                        .to( CYPHERS_ES.getName() )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .endEntity()
                    .addEntity( CYPHERS_ALIAS)
                        .to( CYPHERS_ES.getName() )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .endEntity()
                    .endEntities()
                .done();
        // @formatter:on
    }

    @Test(
            expected = IllegalStateException.class )
    public void testNoDuplicateProperties() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .to( CYPHERS_ES.getName() )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .endEntity()
                    .endEntities()
                .done();
        // @formatter:on
    }

    @Test(
            expected = IllegalStateException.class )
    public void testAssociationSrcDstAliasesMustExist() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .to( CYPHERS_ES.getName() )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .endEntity()
                    .endEntities()
                .createAssociations()
                    .addAssociation( ASSOCIATION_ALIAS )
                        .to( ASSOCIATION_ES.getName() )
                        .fromEntity( CYPHERS_ALIAS )
                        .toEntity( MORE_CYPHERS_ALIAS )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( 0 ) ).ok()
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on
    }

    public static Flight getFlight() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .to( CYPHERS_ES.getName() )
                        .addProperty( ALGO_PT.getType() ).extractor( row -> row.get( "0" ) ).ok()
                        .addProperty( MODE_PT.getType() ).extractor( row -> row.get( "1" ) ).ok()
                        .addProperty( CYPHER_HASH_PT.getType() )
                            .value( ( row, hasher ) -> {
                                hasher.putString( row.get( "0" ), Charsets.UTF_8 );
                                hasher.putString( row.get( "1" ), Charsets.UTF_8 );
                                hasher.putString( row.get( "2" ), Charsets.UTF_8 );
                            } )
                            .ok()
                        .endEntity()
                    .addEntity( MORE_CYPHERS_ALIAS )
                        .to( MORE_CYPHERS_ES.getName() )
                        .addProperty( KEY_SIZE_PT.getType() ).extractor( row -> row.get( "2" ) ).ok()
                        .addProperty( MODE_PT.getType() ).extractor( row -> row.get( "1" ) ).ok()
                        .endEntity()
                    .endEntities()
                .createAssociations()
                    .addAssociation( ASSOCIATION_ALIAS )
                        .to( ASSOCIATION_ES.getName() )
                        .fromEntity( CYPHERS_ALIAS )
                        .toEntity( MORE_CYPHERS_ALIAS )
                        .addProperty( ID_PT.getType() ).extractor( row -> row.get( 3 ) ).ok()
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        return flight;
    }

    @BeforeClass
    public static void initPayload() throws IOException {
        URL url = Resources.getResource( "cyphers.csv" );
        payload = StreamUtil.stream( () -> {
            try {
                return newDefaultMapper()
                        .readerFor( Map.class )
                        .with( newDefaultSchemaFromHeader() )
                        .readValues( url );
            } catch ( IOException e ) {
                logger.error( "Unable to read csv file", e );
                return ImmutableList.<Map<String, String>>of().iterator();
            }
        } );

        //        payload = sparkSession
        //                .read()
        //                .format( "com.databricks.spark.csv" )
        //                .option( "header", "true" )
        //                .load( url.getPath() );

        // initializeTypes();
    }
}
