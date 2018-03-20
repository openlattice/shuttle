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

import com.dataloom.mappers.ObjectMappers;
import com.openlattice.data.serializers.FullQualifiedNameJacksonDeserializer;
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class RequiredEdmElementsManagerTest {

    static {
        FullQualifiedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifiedNameJacksonSerializer.registerWithMapper( ObjectMappers.getYamlMapper() );
    }

    @Test
    public void readAndCompare() throws IOException {

        FileInputStream inputStream = new FileInputStream( "src/test/resources/RequiredEdmElementsManager.test.yaml" );
        RequiredEdmElements actual = ObjectMappers.getYamlMapper().readValue(
                inputStream,
                RequiredEdmElements.class
        );
        inputStream.close();

        Assert.assertTrue( actual.getPropertyTypes().size() == 1 );
        Assert.assertTrue( actual.getPropertyTypes().iterator().next().getProperties().size() == 3 );
    }
}
