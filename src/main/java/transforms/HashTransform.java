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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.openlattice.shuttle.transformations.Transformation;
import com.openlattice.shuttle.util.Constants;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.openlattice.shuttle.transformations.Transformation.TRANSFORM;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class HashTransform extends Transformation<Object> {
    private final List<String> columns;
    private final HashType     hashFunction;

    public enum HashType {murmur128, sha256}

    ;

    private final HashFunction hf;

    @JsonCreator
    public HashTransform(
            @JsonProperty( Constants.COLUMNS ) List<String> columns,
            @JsonProperty( Constants.HASH_FUNCTION ) HashType hashFunction ) {
        this.columns = columns;
        this.hashFunction = hashFunction == null ? HashType.sha256 : hashFunction ;
        switch ( this.hashFunction ) {
            case murmur128:
                hf = Hashing.murmur3_128();
                break;
            case sha256:
                hf = Hashing.sha256();
                break;
            default:
                hf = Hashing.sha256();
        }
    }

    @JsonProperty( Constants.HASH_FUNCTION )
    public HashType getHashFunction() {
        return hashFunction;
    }

    @JsonProperty( Constants.COLUMNS )
    public List<String> getColumns() {
        return columns;
    }

    @Override
    public Object apply( Object input ) {
        if (input instanceof Map){
            Map<String, String> row = (Map) input;
            Hasher hasher = hf.newHasher();
            for ( String s : columns ) {
                if ( !StringUtils.isBlank( row.get( s ) ) ) {
                    hasher.putString( row.get( s ), Charsets.UTF_8 );
                }
            }
            return hasher.hash().toString();
        }

        if (input instanceof String) {
            return super.apply( input );
        }

        throw  new IllegalArgumentException("This isn't a known input datatype");
    }

    @Override
    public Object applyValue( String inString ) {
        Hasher hasher = hf.newHasher();
        hasher.putString( inString,  Charsets.UTF_8 );
        return hasher.hash().toString();
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !( o instanceof HashTransform ) ) {
            return false;
        }
        HashTransform that = (HashTransform) o;
        return Objects.equals( columns, that.columns ) &&
                Objects.equals( hashFunction, that.hashFunction ) &&
                Objects.equals( hf, that.hf );
    }

    @Override
    public int hashCode() {

        return Objects.hash( columns, hashFunction, hf );
    }
}
