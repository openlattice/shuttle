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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.shuttle.config.IntegrationConfig;
import com.openlattice.shuttle.payload.JdbcPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dataloom.mappers.ObjectMappers;
import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ShuttleServer {

    private static final Logger                      logger      = LoggerFactory.getLogger( ShuttleServer.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException, IOException {

        if (args.length < 1)
        {
            System.out.println( "Hello, Shuttle!" );

        } else {

            // get flight
            System.out.println(args.toString());

            final String yamlfile = args[0];
            ObjectMapper yaml = ObjectMappers.getYamlMapper();
            FullQualifiedNameJacksonSerializer.registerWithMapper( yaml );

            Flight flight = null;
            try {
                flight = yaml.readValue( new File( yamlfile ), Flight.class );
            } catch (Exception e) {
                e.printStackTrace();
            }
            Map<Flight, Payload> flights = new LinkedHashMap<>( 2 );
            logger.info("This is the JSON for the flight. {}", yaml.writeValueAsString(flight));

            // get jwt

            final String jwtToken;

            if (args.length == 4 | args.length == 6){
                jwtToken = MissionControl.getIdToken( args[1], args[2] );
            } else {
                jwtToken = args[1];
            }
            logger.info("JWT for this flight. {}", jwtToken);

            Shuttle shuttle = new Shuttle( environment, jwtToken );

            // get data

            final String dataPath;
            Integer offset = args.length-1; // offset for next args

            if (args.length == 3 | args.length == 4) {

                // get datapath (offset for username/pw vs jwt
                dataPath = args[offset];

                // get payload
                Payload Payload = new SimplePayload( args[offset] );
                logger.info("datafile: "+args[offset]);
                flights.put( flight, Payload );
                shuttle.launchPayloadFlight( flights );

            } else {

                HikariDataSource hds = ObjectMappers.getYamlMapper()
                        .readValue(new File(args[offset+1]), IntegrationConfig.class)
                        .getHikariDatasource(args[offset]);
                Payload arPayload = new JdbcPayload(hds, args[offset+2]);
                flights.put( flight, arPayload );
                shuttle.launchPayloadFlight( flights );

            }
        }
    }
}
