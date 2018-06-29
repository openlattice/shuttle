/*
 * Copyright (C) 2018. OpenLattice, Inc.
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
 *
 */

package transforms;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.openlattice.shuttle.Transformation;
import com.openlattice.shuttle.util.Constants;
import java.util.List;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HashTransform extends Transformation<Map<String, String>, Object> {
    private final List<String> columns;
    private final String       hashFunction;
    private final HashFunction hf;

    @JsonCreator
    public HashTransform(
            @JsonProperty( Constants.COLUMNS ) List<String> columns,
            @JsonProperty( Constants.HASH_FUNCTION ) String hashFunction ) {
        this.columns = columns;
        this.hashFunction = hashFunction;
        switch ( hashFunction ) {
            default:
            case "":
            case "murmur128":
                hf = Hashing.murmur3_128();
                break;
            case "sha256":
                hf = Hashing.sha256();
                break;
        }
    }

    @JsonProperty( Constants.HASH_FUNCTION )
    public String getHashFunction() {
        return hashFunction;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override public Object apply( Map<String, String> row ) {
        Hasher hasher = hf.newHasher();
        columns.stream().map( row::get ).forEach( s -> hasher.putString( s, Charsets.UTF_8 ) );
        return hasher.hash().toString();
    }
}
