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
 *
 */

package com.openlattice.shuttle;

import com.google.common.collect.ImmutableMap;
import com.openlattice.client.serialization.SerializableFunction;
import com.openlattice.mapstores.TestDataFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class RowAdapterTest {
    @Test
    public void testRowAdapter() {
        final Map<String, Object> row = ImmutableMap.of( "test", TestDataFactory.random( 5 ) );
        final SerializableFunction<Row, Object> sf = r -> r.getAs( "test" );

        RowAdapter adapter = new RowAdapter( sf );
        Assert.assertEquals( row.get( "test" ), adapter.apply( row ) );
    }
}
