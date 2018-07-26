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
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.dataloom.mappers.ObjectMappers;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

public class ShuttleServer {

    private static final Logger                      logger      = LoggerFactory.getLogger( ShuttleServer.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    public static void main( String[] args )  throws InterruptedException, JsonProcessingException {
        if (args.length < 3)
        {
            System.out.println( "Hello, Shuttle!" );
        } else {

            final String arpath = args[1];
            logger.info(arpath);
            final String jwtToken = args[2];
            final String yamlfile = args[3];

            SimplePayload arPayload = new SimplePayload( arpath );

            ObjectMapper yaml = ObjectMappers.getYamlMapper();
            FullQualifiedNameJacksonSerializer.registerWithMapper( yaml );

            Flight arFlight = null;
            try {
                arFlight = yaml.readValue( new File( yamlfile ), Flight.class );
            } catch (Exception e) {
                e.printStackTrace();
            }

            Map<Flight, Payload> flights = new LinkedHashMap<>( 2 );
            logger.info("This is the JSON for the flight. {}", yaml.writeValueAsString(arFlight));
            flights.put( arFlight, arPayload );
            Shuttle shuttle = new Shuttle( environment, jwtToken );
            shuttle.launchPayloadFlight( flights );

            System.out.println( "Hello, Big Shuttle!" );

        }

    }
}
