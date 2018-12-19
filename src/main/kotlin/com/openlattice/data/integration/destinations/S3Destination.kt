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

package com.openlattice.data.integration.destinations

import com.openlattice.data.*
import com.openlattice.data.integration.*
import com.openlattice.data.integration.Entity
import com.openlattice.data.util.PostgresDataHasher
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class S3Destination(
        private val dataApi: DataApi,
        private val s3Api: S3Api,
        private val dataIntegrationApi: DataIntegrationApi
) : IntegrationDestination {
    override fun integrateEntities(
            data: Set<Entity>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        val (s3entities, values) = data.flatMap { entity ->
            val entityKeyId = entityKeyIds[entity.key]!!
            entity.details.entries.flatMap { (propertyTypeId, properties) ->
                properties.map {
                    S3EntityData(
                            entity.entitySetId,
                            entityKeyId,
                            propertyTypeId,
                            PostgresDataHasher.hashObjectToHex(it, EdmPrimitiveTypeKind.Binary)
                    ) to it
                }
            }
        }.unzip()

        dataIntegrationApi
                .generatePresignedUrls(s3entities)
                .zip(values)
                .parallelStream()
                .forEach { s3Api.writeToS3(it.first, it.second as ByteArray) }

        return values.size.toLong()
    }

    override fun integrateAssociations(
            data: Set<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        val (s3entities, values) = data.flatMap { entity ->
            val entityKeyId = entityKeyIds[entity.key]!!
            entity.details.entries.flatMap { (propertyTypeId, properties) ->
                properties.map {
                    S3EntityData(
                            entity.key.entitySetId,
                            entityKeyId,
                            propertyTypeId,
                            PostgresDataHasher.hashObjectToHex(it, EdmPrimitiveTypeKind.Binary)
                    ) to it
                }
            }
        }.unzip()

        dataIntegrationApi
                .generatePresignedUrls(s3entities)
                .zip(values)
                .parallelStream()
                .forEach { s3Api.writeToS3(it.first, it.second as ByteArray) }

        val entities = data.map {
            val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds[it.src])
            val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds[it.dst])
            val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds[it.key])
            DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
        }.toSet()
        return values.size.toLong() + dataApi.createAssociations(entities).toLong()
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.S3
    }
}