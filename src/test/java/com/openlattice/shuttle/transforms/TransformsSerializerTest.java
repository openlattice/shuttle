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

package com.openlattice.shuttle.transforms;

import com.google.common.collect.ImmutableList;
import com.openlattice.serializer.AbstractJacksonSerializationTest;
import com.openlattice.shuttle.transformations.Transformations;
import com.openlattice.shuttle.transforms.TransformsSerializerTest.OptionalListOfTransforms;

import java.util.Objects;
import java.util.Optional;

import transforms.PrefixTransform;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class TransformsSerializerTest extends AbstractJacksonSerializationTest<OptionalListOfTransforms> {
    @Override protected OptionalListOfTransforms getSampleData() {
        OptionalListOfTransforms olt = new OptionalListOfTransforms();
        Transformations transforms = new Transformations(
                ImmutableList.of( new PrefixTransform( "COWBELL_" ),
                        new HashTransform( ImmutableList.of( "algo", "mode", "keySize" ), HashTransform.HashType.murmur128 ) ) );
        olt.field = Optional.of( transforms );

        return olt;
    }

    @Override protected Class<OptionalListOfTransforms> getClazz() {
        return OptionalListOfTransforms.class;
    }

    public static class OptionalListOfTransforms {
        public Optional<Transformations> field;

        @Override public String toString() {
            return "OptionalListOfTransforms{" +
                    "field=" + field +
                    '}';
        }

        @Override public boolean equals( Object o ) {
            if ( this == o ) { return true; }
            if ( !( o instanceof OptionalListOfTransforms ) ) { return false; }
            OptionalListOfTransforms that = (OptionalListOfTransforms) o;
            return Objects.equals( field, that.field );
        }

        @Override public int hashCode() {

            return Objects.hash( field );
        }
    }
}
