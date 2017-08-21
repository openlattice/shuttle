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

import com.dataloom.client.serialization.SerializableFunction;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.spark.sql.Row;

public class HashingMapper {

    public static SerializableFunction<Row, String> getMapper( Funnel<Row> funnel ) {

        return getMapper( funnel, Hashing.sha256() );
    }

    public static SerializableFunction<Row, String> getMapper( Funnel<Row> funnel, HashFunction hashFunction ) {

        return row -> {

            Hasher hasher = hashFunction.newHasher();
            funnel.funnel( row, hasher );
            return hasher.hash().toString();
        };
    }
}
