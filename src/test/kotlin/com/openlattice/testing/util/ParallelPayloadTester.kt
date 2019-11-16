/*
 * Copyright (C) 2019. OpenLattice, Inc.
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

package com.openlattice.testing.util

import org.junit.Test
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class ParallelPayloadTester {
    @Test
    fun testPayload() {
        Payload( 100_000_000 ) .asSequence().chunked(10).asStream().parallel()
                .map { it.sum() }
                .forEach {
                    println(it)
                }
    }
}

class Payload(val limit: Long) : Iterable<Long> {
    override fun iterator(): Iterator<Long> {
        return object : Iterator<Long> {
            override fun next(): Long {
                return ++i
            }

            override fun hasNext(): Boolean {
                return i < limit
            }

            var i = 0L
        }
    }
}