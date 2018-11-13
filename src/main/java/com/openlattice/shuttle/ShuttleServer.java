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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dataloom.mappers.ObjectMappers;

import java.io.File;
import java.io.IOException;
import java.util.*;


/**
 * @deprecated Use {@link ShuttleKt} instead
 */
@Deprecated
public class ShuttleServer {

    private static final Logger                      logger      = LoggerFactory.getLogger( ShuttleServer.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;

    public static void main( String[] args ) throws InterruptedException, IOException {

        if ( args.length < 1 ) {
            System.out.println( "Hello, Shuttle!" );

        } else {

            // get flight
            System.out.println( args.toString() );

            int argIndex = 0;

            final String yamlfile = args[ argIndex++ ]; // 0
            ObjectMapper yaml = ObjectMappers.getYamlMapper();
            FullQualifiedNameJacksonSerializer.registerWithMapper( yaml );

            Flight flight = null;
            try {
                flight = yaml.readValue( new File( yamlfile ), Flight.class );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            Map<Flight, Payload> flights = new LinkedHashMap<>( 2 );
            logger.info( "This is the JSON for the flight. {}", yaml.writeValueAsString( flight ) );

            // get jwt
            final String jwtToken;

            if ( args.length == 6 || ( args.length == 5 && args[4].equals( "false" ) ) ) {
                jwtToken = MissionControl.getIdToken( args[ argIndex++ ], args[ argIndex++ ] ); // 1, 2
            } else {
                jwtToken = args[ argIndex++ ]; // 1
            }
            logger.info( "JWT for this flight. {}", jwtToken );

            Shuttle shuttle = new Shuttle( environment, jwtToken );

            // get datapath
            final String dataPath = args[ argIndex++ ]; // 2 or 3

            // get whether to create entitysets automatically or not
            final boolean createEntitySets = Boolean.valueOf( args[ argIndex++ ] ); // 3 or 4

            // get the emails/contacts for automatic entity set creation
            final Set<String> contacts;
            if( createEntitySets ) {
                if( environment == RetrofitFactory.Environment.PRODUCTION ) {
                    throw new IllegalArgumentException(
                            "It is not allowed to automatically create entity sets on " +
                                    RetrofitFactory.Environment.PRODUCTION + " environment" );
                }

                if(argIndex == args.length) {
                    throw new IllegalArgumentException(
                            "Can't create entity sets automatically without contacts provided" );
                }
                contacts = Set.of( args[argIndex++].split( "," ) ); // 4 or 5
            } else {
                contacts = new HashSet<>(  );
            }

            // get payload
            Payload Payload = new SimplePayload( dataPath );
            logger.info( "datafile: " + dataPath );
            flights.put( flight, Payload );
            shuttle.launchPayloadFlight( flights, createEntitySets, contacts );
         }
    }
}
