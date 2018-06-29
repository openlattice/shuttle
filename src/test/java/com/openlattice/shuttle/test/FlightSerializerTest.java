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

import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import com.openlattice.shuttle.Flight;
import org.junit.BeforeClass;

public class FlightSerializerTest extends AbstractJacksonSerializationTest<Flight> {

    @Override
    protected Flight getSampleData() {
        return ShuttleTest.getFlight();
    }

    @Override
    protected Class<Flight> getClazz() {
        return Flight.class;
    }

    @BeforeClass
    public static void registerModules() {
        registerModule( FullQualifiedNameJacksonSerializer::registerWithMapper );
//        registerModule( FullQualifiedNameJacksonDeserializer::registerWithMapper );
        //        registerModule( JacksonLambdaSerializer::registerWithMapper );
        //        registerModule( JacksonLambdaDeserializer::registerWithMapper );
    }
}
