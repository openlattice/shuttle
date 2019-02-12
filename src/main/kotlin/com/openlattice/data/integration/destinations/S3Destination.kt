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
import org.slf4j.LoggerFactory
import java.lang.ClassCastException
import java.lang.Exception
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Supplier
import java.util.stream.Collectors
import kotlin.streams.asSequence

private val logger = LoggerFactory.getLogger(S3Destination::class.java)

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
//        val (s3entities, values)
               val s3entities = data.flatMap { entity ->
            val entityKeyId = entityKeyIds.getValue(entity.key)
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
        }

        var s3eds = s3entities

        while( s3eds.isNotEmpty() ) {
            val (s3entities, values) = s3eds.unzip()
            val presignedUrls = dataIntegrationApi.generatePresignedUrls(s3entities)

            s3eds = s3entities
                    .mapIndexed { index, s3EntityData ->
                        Triple(s3EntityData, presignedUrls[index], values[index])
                    }
                    .parallelStream()
                    .filter { (s3ed, url, bytes) ->
                        try {
                            s3Api.writeToS3(url, bytes as ByteArray)
                            false
                        } catch (ex: Exception) {
                            if (ex is ClassCastException) {
                                logger.error("Expected byte array, but found wrong data type for upload.", ex)
                                kotlin.system.exitProcess(2)
                            } else {
                                logger.error("Encountered an issue when uploading data. Retrying...", ex)
                                true
                            }
                        }
                    }
                    .map {(s3ed, _, bytes ) ->
                        s3ed to bytes
                    }.collect(Collectors.toList())
        }

        return s3entities.size.toLong()
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