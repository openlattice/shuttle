/*
 * Copyright (C) 2020. OpenLattice, Inc.
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

package com.openlattice.shuttle.destinations

import com.openlattice.data.DataEdgeKey
import com.openlattice.data.PropertyUpdateType
import com.openlattice.data.S3Api
import com.openlattice.data.integration.S3EntityData

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresS3Destination(
        private val postgresDestination: PostgresDestination,
        s3Api: S3Api,
        generatePresignedUrlsFun: (List<S3EntityData>, PropertyUpdateType) -> List<String>
) : BaseS3Destination(s3Api, generatePresignedUrlsFun) {
    override fun createAssociations(entities: Set<DataEdgeKey>): Long {
        return postgresDestination.createEdges(entities)
    }
}