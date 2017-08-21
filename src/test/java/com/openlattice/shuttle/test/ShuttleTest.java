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

import com.dataloom.client.ApiFactory;
import com.dataloom.client.ApiFactoryFactory;
import com.dataloom.data.DataApi;
import com.dataloom.edm.EdmApi;
import com.dataloom.sync.SyncApi;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URL;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShuttleTest extends ShuttleTestBootstrap {

    private static Dataset<Row> payload;

    private static String CYPHERS_ES_NAME      = "Cyphers";
    private static String MORE_CYPHERS_ES_NAME = "More Cyphers";
    private static String ASSOCIATION_ES_NAME  = "Is Also A Cypher";
    private static String CYPHERS_ALIAS        = "cyphers";
    private static String MORE_CYPHERS_ALIAS   = "moreCyphers";
    private static String ASSOCIATION_ALIAS    = "cypherToCypher";

    private static UUID CYPHER_ES_ID            = UUID.randomUUID();
    private static UUID MORE_CYPHERS_ES_ID      = UUID.randomUUID();
    private static UUID ASSOCIATION_ES_ID       = UUID.randomUUID();
    private static UUID CYPHER_ES_SYNC_ID       = UUIDs.timeBased();
    private static UUID MORE_CYPHERS_ES_SYNC_ID = UUIDs.timeBased();
    private static UUID ASSOCIATION_ES_SYNC_ID  = UUIDs.timeBased();

    private static FullQualifiedName CYPHER_ET_FQN      = new FullQualifiedName( "KRYPTO", "Cypher" );
    private static FullQualifiedName ASSOCIATION_ET_FQN = new FullQualifiedName( "KRYPTO", "CypherToCypher" );
    private static FullQualifiedName ALGO_PT_FQN        = new FullQualifiedName( "KRYPTO", "Algo" );
    private static FullQualifiedName MODE_PT_FQN        = new FullQualifiedName( "KRYPTO", "Mode" );
    private static FullQualifiedName KEY_SIZE_PT_FQN    = new FullQualifiedName( "KRYPTO", "KeySize" );
    private static FullQualifiedName CYPHER_HASH_PT_FQN = new FullQualifiedName( "KRYPTO", "CypherHash" );
    private static FullQualifiedName ID_PT_FQN          = new FullQualifiedName( "KRYPTO", "id" );

    public static Map<FullQualifiedName, UUID> PROPERTIES = Maps
            .asMap(
                    ImmutableSet.copyOf(
                            Arrays.asList(
                                    CYPHER_ET_FQN,
                                    ALGO_PT_FQN,
                                    MODE_PT_FQN,
                                    KEY_SIZE_PT_FQN,
                                    CYPHER_HASH_PT_FQN,
                                    ID_PT_FQN
                            )
                    ),
                    fqn -> UUID.randomUUID()
            );

    public static Flight getFlight() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .ofType( CYPHER_ET_FQN )
                        .to( CYPHERS_ES_NAME )
                        .key( ALGO_PT_FQN, MODE_PT_FQN )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .addProperty( MODE_PT_FQN ).value( row -> row.get( 1 ) ).ok()
                        .addProperty( CYPHER_HASH_PT_FQN )
                            .value( ( row, hasher ) -> {
                                hasher.putString( row.getString( 0 ), Charsets.UTF_8 );
                                hasher.putString( row.getString( 1 ), Charsets.UTF_8 );
                                hasher.putString( row.getString( 2 ), Charsets.UTF_8 );
                            } )
                            .ok()
                        .ok()
                    .addEntity( MORE_CYPHERS_ALIAS )
                        .ofType( CYPHER_ET_FQN )
                        .to( MORE_CYPHERS_ES_NAME )
                        .key( KEY_SIZE_PT_FQN )
                        .addProperty( KEY_SIZE_PT_FQN ).value( row -> row.get( 2 ) ).ok()
                        .addProperty( MODE_PT_FQN ).value( row -> row.get( 1 ) ).ok()
                        .ok()
                    .ok()
                .createAssociations()
                    .addAssociation( ASSOCIATION_ALIAS )
                        .ofType( ASSOCIATION_ET_FQN )
                        .to( ASSOCIATION_ES_NAME )
                        .fromEntity( CYPHERS_ALIAS )
                        .toEntity( MORE_CYPHERS_ALIAS )
                        .key( ID_PT_FQN )
                        .addProperty( ID_PT_FQN ).value( row -> row.get( 3 ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        return flight;
    }

    @BeforeClass
    public static void initPayload() {

        URL url = Resources.getResource( "cyphers.csv" );
        payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( url.getPath() );
    }

    @Test
    public void testFlight() throws InterruptedException {

        ApiFactoryFactory apiFactorySupplier = () -> {

            EdmApi edmApi = mock( EdmApi.class );
            DataApi mockDataApi = mock( DataApi.class );
            SyncApi mockSyncApi = mock( SyncApi.class );
            ApiFactory mockApiFactory = mock( ApiFactory.class );

            when( mockApiFactory.create( DataApi.class ) )
                    .thenAnswer( Answers.incrementCreateDataApiCount( mockDataApi ) );

            when( mockApiFactory.create( EdmApi.class ) )
                    .thenReturn( edmApi );

            when( mockApiFactory.create( SyncApi.class ) )
                    .thenReturn( mockSyncApi );

            when( edmApi.getEntitySetId( CYPHERS_ES_NAME ) ).thenReturn( CYPHER_ES_ID );
            when( edmApi.getEntitySetId( MORE_CYPHERS_ES_NAME ) ).thenReturn( MORE_CYPHERS_ES_ID );
            when( edmApi.getEntitySetId( ASSOCIATION_ES_NAME ) ).thenReturn( ASSOCIATION_ES_ID );
            when( mockSyncApi.acquireSyncId( CYPHER_ES_ID ) ).thenReturn( CYPHER_ES_SYNC_ID );
            when( mockSyncApi.acquireSyncId( MORE_CYPHERS_ES_ID ) ).thenReturn( MORE_CYPHERS_ES_SYNC_ID );
            when( mockSyncApi.acquireSyncId( ASSOCIATION_ES_ID ) ).thenReturn( ASSOCIATION_ES_SYNC_ID );

            PROPERTIES.entrySet()
                    .forEach( e -> when( edmApi.getPropertyTypeId( e.getKey().getNamespace(), e.getKey().getName() ) )
                            .thenReturn( e.getValue() ) );

            doAnswer( Answers.incrementCreateDataInvocationCount() )
                    .when( mockDataApi ).createEntityAndAssociationData( Mockito.any() );

            when( mockDataApi.acquireSyncTicket( Mockito.any(), Mockito.any() ) ).thenReturn( new UUID( 1, 1 ) );
            // Mockito.verify( mockDataApi, Mockito.atLeastOnce() ).releaseSyncTicket( new UUID( 1, 1 ) );
            return mockApiFactory;
        };

        // @formatter:off
        Flight flight = getFlight();
        // @formatter:on

        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();
        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( apiFactorySupplier );
        shuttle.launch( flights );

        Assert.assertEquals( 6, Answers.getCreateDataInvocationCount() );
        Assert.assertEquals( 2, Answers.getCreateDataApiInvocationCount() );
    }

    @Test( expected = IllegalStateException.class )
    public void testNoEntities() {

        Flight flight = Flight.newFlight().done();
    }

    @Test( expected = IllegalStateException.class )
    public void testNoProperties() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( CYPHERS_ALIAS).ofType( CYPHER_ET_FQN ).to( CYPHERS_ES_NAME ).ok()
                .ok().done();
        // @formatter:on
    }

    @Test( expected = IllegalStateException.class )
    public void testNoDuplicateEntities() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS)
                        .ofType( CYPHER_ET_FQN )
                        .to( CYPHERS_ES_NAME )
                        .key( ALGO_PT_FQN )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .ok()
                    .addEntity( CYPHERS_ALIAS)
                        .ofType( CYPHER_ET_FQN )
                        .to( CYPHERS_ES_NAME )
                        .key( ALGO_PT_FQN )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on
    }

    @Test( expected = IllegalStateException.class )
    public void testNoDuplicateProperties() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .ofType( CYPHER_ET_FQN )
                        .to( CYPHERS_ES_NAME )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on
    }

    @Test( expected = IllegalStateException.class )
    public void testAssociationSrcDstAliasesMustExist() {

        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                    .addEntity( CYPHERS_ALIAS )
                        .ofType( CYPHER_ET_FQN )
                        .to( CYPHERS_ES_NAME )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .ok()
                    .ok()
                .createAssociations()
                    .addAssociation( ASSOCIATION_ALIAS )
                        .ofType( ASSOCIATION_ET_FQN )
                        .to( ASSOCIATION_ES_NAME )
                        .fromEntity( CYPHERS_ALIAS )
                        .toEntity( MORE_CYPHERS_ALIAS )
                        .key( ALGO_PT_FQN )
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .addProperty( ALGO_PT_FQN ).value( row -> row.get( 0 ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on
    }
}
