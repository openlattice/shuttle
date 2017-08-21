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

import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.serializer.AbstractJacksonSerializationTest;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.serialization.JacksonLambdaDeserializer;
import com.openlattice.shuttle.serialization.JacksonLambdaSerializer;

public class FlightSerializerTest extends AbstractJacksonSerializationTest<Flight> {

    static {
        registerModule( FullQualifedNameJacksonDeserializer::registerWithMapper );
        registerModule( FullQualifedNameJacksonSerializer::registerWithMapper );
        registerModule( JacksonLambdaSerializer::registerWithMapper );
        registerModule( JacksonLambdaDeserializer::registerWithMapper );
    }

    @Override
    protected Flight getSampleData() {
        return ShuttleTest.getFlight();
    }

    @Override
    protected Class<Flight> getClazz() {
        return Flight.class;
    }
}
