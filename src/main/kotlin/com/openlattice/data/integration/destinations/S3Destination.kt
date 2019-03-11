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
import com.openlattice.shuttle.MissionControl
import org.apache.commons.lang3.RandomUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.lang.ClassCastException
import java.lang.Exception
import java.util.*
import java.util.stream.Collectors
import kotlin.math.max

private val logger = LoggerFactory.getLogger(S3Destination::class.java)
const val MAX_DELAY_MILLIS = 60 * 60 * 1000L
const val MAX_RETRY_COUNT = 22

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

        uploadToS3WithRetry(s3entities)

        return s3entities.size.toLong()
    }

    private fun getS3Urls(s3entities: List<S3EntityData>): List<String> {
        var currentDelayMillis = 1L
        var currentRetryCount = 0
        while (true) {
            val maybeUrls: List<String>? = dataIntegrationApi.generatePresignedUrls(s3entities)
            if (maybeUrls == null) {
                currentRetryCount++
                if (currentRetryCount <= MAX_RETRY_COUNT) {
                    Thread.sleep(currentDelayMillis)
                    currentDelayMillis = max(
                            MAX_DELAY_MILLIS,
                            (currentDelayMillis * RandomUtils.nextDouble(1.25, 2.0).toLong())
                    )
                } else {
                    throw IllegalStateException("Unable to retrieve presigned urls. Maybe prod is down?")
                }
            } else {
                return maybeUrls
            }
        }

    }

    override fun integrateAssociations(
            data: Set<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        val s3entities = data.flatMap { entity ->
            val entityKeyId = entityKeyIds.getValue(entity.key)
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
        }

        uploadToS3WithRetry(s3entities)

        val entities = data.map {
            val srcDataKey = EntityDataKey(it.src.entitySetId, entityKeyIds[it.src])
            val dstDataKey = EntityDataKey(it.dst.entitySetId, entityKeyIds[it.dst])
            val edgeDataKey = EntityDataKey(it.key.entitySetId, entityKeyIds[it.key])
            DataEdgeKey(srcDataKey, dstDataKey, edgeDataKey)
        }.toSet()
        return s3entities.size.toLong() + dataApi.createAssociations(entities).toLong()
    }

    private fun uploadToS3WithRetry(s3entities: List<Pair<S3EntityData, Any>>) {
        var s3eds = s3entities

        var currentDelayMillis = 1L
        var currentRetryCount = 0

        while (s3eds.isNotEmpty()) {
            val (s3entities, values) = s3eds.unzip()
            val presignedUrls = getS3Urls(s3entities)

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
                                logger.error(
                                        "Expected byte array, but found wrong data type for upload (entitySetId=${s3ed.entitySetId}, entityKeyId=${s3ed.entityKeyId}, PropertType=${s3ed.propertyTypeId}).",
                                        ex
                                )
                                MissionControl.fail(2)
                            } else {
                                logger.warn(
                                        "Encountered an issue when uploading data (entitySetId=${s3ed.entitySetId}, entityKeyId=${s3ed.entityKeyId}, PropertType=${s3ed.propertyTypeId}). Retrying...",
                                        ex
                                )
                                true
                            }
                        }
                    }
                    .map { (s3ed, _, bytes) ->
                        s3ed to bytes
                    }.collect(Collectors.toList())

            if (s3eds.isNotEmpty()) {
                currentRetryCount++
                if (currentRetryCount <= MAX_RETRY_COUNT) {
                    Thread.sleep(currentDelayMillis)
                    currentDelayMillis = max(
                            MAX_DELAY_MILLIS,
                            (currentDelayMillis * RandomUtils.nextDouble(1.25, 2.0)).toLong()
                    )

                } else {
                    throw IllegalStateException("Unable to retrieve presigned urls. Maybe prod is down?")
                }
            }
        }
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.S3
    }
}