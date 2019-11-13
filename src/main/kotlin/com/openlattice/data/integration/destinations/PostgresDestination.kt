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

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.google.common.collect.Multimaps
import com.openlattice.data.EntityKey
import com.openlattice.data.UpdateType
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.Entity
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.storage.buildUpsertEntitiesAndLinkedData
import com.openlattice.data.storage.getPartition
import com.openlattice.data.storage.updateVersionsForPropertyTypesInEntitiesInEntitySet
import com.openlattice.data.storage.upsertPropertyValueSql
import com.openlattice.data.util.PostgresDataHasher
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.postgres.JsonDeserializer
import com.openlattice.postgres.PostgresArrays
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.slf4j.LoggerFactory
import java.security.InvalidParameterException
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class PostgresDestination(
        private val entitySets: Map<UUID, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<UUID, PropertyType>,
        private val hds: HikariDataSource
) : IntegrationDestination {
    companion object {
        private val logger = LoggerFactory.getLogger(PostgresDestination::class.java)
        private val mapper = ObjectMappers.newJsonMapper()
    }

    override fun integrateEntities(
            data: Set<Entity>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {

        return hds.connection.use { connection ->

            data
                    .groupBy({ it.entitySetId }, { normalize(entityKeyIds, it) })
                    .map { (entitySetId, entities) ->
                        val entitySet = entitySets.getValue(entitySetId)

                        val partitions = entitySet.partitions.toList()
                        val partitionsVersion = entitySet.partitionsVersion

                        val baseVersion = System.currentTimeMillis()
                        val tombstoneVersion = -baseVersion
                        val writeVersion = baseVersion + 1
                        val relevantPropertyTypes = entityTypes
                                .getValue(entitySets.getValue(entitySetId).entityTypeId)
                                .properties
                                .associateWith(propertyTypes::getValue)
                        val propertyTypeIdsArr = PostgresArrays.createUuidArray(connection, relevantPropertyTypes.keys)
                        val writeVersionArray = PostgresArrays.createLongArray(connection, writeVersion)

                        entities.groupBy { getPartition(it.first, partitions) }
                                .map { (partition, entityPairs) ->
                                    val entityMap = entityPairs.toMap()
                                    val entityKeyIdsArr = PostgresArrays.createUuidArray(connection, entityMap.keys)
                                    val partitionsArr = PostgresArrays.createIntArray(
                                            connection, entityMap.keys.map { getPartition(it, partitions) }
                                    )

                                    if (updateTypes.getValue(entitySetId) == UpdateType.Replace) {
                                        tombstone(
                                                connection,
                                                entitySet,
                                                partitionsVersion,
                                                entityKeyIdsArr,
                                                partitionsArr,
                                                propertyTypeIdsArr,
                                                tombstoneVersion
                                        )
                                    }

                                    upsertEntities(
                                            connection,
                                            entitySet,
                                            partition,
                                            partitionsVersion,
                                            entityMap,
                                            relevantPropertyTypes,
                                            writeVersionArray,
                                            writeVersion
                                    )

                                    commitEntities(
                                            connection,
                                            entitySetId,
                                            partition,
                                            entityKeyIdsArr,
                                            writeVersionArray,
                                            writeVersion
                                    )
                                }.sum().toLong()
                    }.sum()
        }

    }

    override fun integrateAssociations(
            data: Set<Association>, entityKeyIds: Map<EntityKey, UUID>, updateTypes: Map<UUID, UpdateType>
    ): Long {
        return integrateEntities(data.map { Entity(it.key, it.details) }.toSet(), entityKeyIds, updateTypes)
    }

    override fun accepts(): StorageDestination {
        return StorageDestination.POSTGRES
    }

    private fun normalize(entityKeyIds: Map<EntityKey, UUID>, entity: Entity): Pair<UUID, Map<UUID, Set<Any>>> {
        val propertyValues = mapper.readValue<Map<UUID, Set<Any>>>(mapper.writeValueAsBytes(entityKeyIds))
        val validatedPropertyValues = Multimaps.asMap(
                JsonDeserializer.validateFormatAndNormalize(propertyValues, propertyTypes) {
                    "Error validating during integration"
                })
        return entityKeyIds.getValue(entity.key) to validatedPropertyValues
    }

    private fun upsertEntities(
            connection: Connection,
            entitySet: EntitySet,
            partition: Int,
            partitionsVersion: Int,
            entities: Map<UUID, Map<UUID, Set<Any>>>,
            propertyTypes: Map<UUID, PropertyType>,
            versionArray: java.sql.Array,
            version: Long,
            entitySetId: UUID = entitySet.id
    ): Int {

        /*
         * We do not need entity level locking as our version field ensures that data is consistent even across
         * transactions in all cases, except deletes (clear is fine) as long entity version is not bumped until
         * all properties are written.
         *
         */

        //Update property values. We use multiple prepared statements in batch while re-using ARRAY[version].
        val upsertPropertyValues = mutableMapOf<UUID, PreparedStatement>()

        return entities.entries.map { (entityKeyId, entityData) ->
            entityData.map { (propertyTypeId, values) ->
                val upsertPropertyValue = upsertPropertyValues.getOrPut(propertyTypeId) {
                    val pt = propertyTypes[propertyTypeId] ?: abortInsert(entitySetId, entityKeyId)
                    connection.prepareStatement(upsertPropertyValueSql(pt))
                }

                values.map { value ->
                    val dataType = propertyTypes.getValue(propertyTypeId).datatype

                    val (propertyHash, insertValue) = getPropertyHash(
                            entitySetId,
                            entityKeyId,
                            propertyTypeId,
                            value,
                            dataType
                    )

                    upsertPropertyValue.setObject(1, entitySetId)
                    upsertPropertyValue.setObject(2, entityKeyId)
                    upsertPropertyValue.setInt(3, partition)
                    upsertPropertyValue.setObject(4, propertyTypeId)
                    upsertPropertyValue.setObject(5, propertyHash)
                    upsertPropertyValue.setObject(6, version)
                    upsertPropertyValue.setArray(7, versionArray)
                    upsertPropertyValue.setInt(8, partitionsVersion)
                    upsertPropertyValue.setObject(9, insertValue)
                    upsertPropertyValue.addBatch()
                }
            }
            upsertPropertyValues.values.map { it.executeBatch().sum() }.sum()
        }.sum()
    }

    private fun tombstone(
            connection: Connection,
            entitySet: EntitySet,
            partitionsVersion: Int,
            entityKeyIdsArr: java.sql.Array,
            partitionsArr: java.sql.Array,
            propertyTypeIdsArr: java.sql.Array,
            version: Long,
            entitySetId: UUID = entitySet.id
    ) {


        val updatePropertyValueVersion = connection.prepareStatement(
                updateVersionsForPropertyTypesInEntitiesInEntitySet()
        )
        val tombstoneLinks = connection.prepareStatement(
                updateVersionsForPropertyTypesInEntitiesInEntitySet(linking = true)
        )

        updatePropertyValueVersion.setLong(1, -version)
        updatePropertyValueVersion.setLong(2, -version)
        updatePropertyValueVersion.setLong(3, -version)
        updatePropertyValueVersion.setObject(4, entitySetId)
        updatePropertyValueVersion.setArray(5, propertyTypeIdsArr)
        updatePropertyValueVersion.setArray(6, entityKeyIdsArr)
        updatePropertyValueVersion.setArray(7, partitionsArr)
        updatePropertyValueVersion.setInt(8, partitionsVersion)
        updatePropertyValueVersion.addBatch()

        tombstoneLinks.setLong(1, -version)
        tombstoneLinks.setLong(2, -version)
        tombstoneLinks.setLong(3, -version)
        tombstoneLinks.setObject(4, entitySetId)
        tombstoneLinks.setArray(5, propertyTypeIdsArr)
        tombstoneLinks.setArray(6, entityKeyIdsArr)
        tombstoneLinks.setArray(7, partitionsArr)
        tombstoneLinks.setInt(8, partitionsVersion)
        tombstoneLinks.addBatch()


        val numUpdated = updatePropertyValueVersion.executeUpdate()
        val linksUpdated = tombstoneLinks.executeUpdate()

        logger.info("Tombstoned $numUpdated properties in ${entitySet.name} with version $version ")
        logger.info("Tombstoned $linksUpdated linked properties in ${entitySet.name} with version $version")

    }

    private fun commitEntities(
            connection: Connection,
            entitySetId: UUID,
            partition: Int,
            entityKeyIdsArr: java.sql.Array,
            versionArray: java.sql.Array,
            version: Long
    ): Int {
//Make data visible by marking new version in ids table.
        val upsertEntities = connection.prepareStatement(buildUpsertEntitiesAndLinkedData())


        upsertEntities.setObject(1, versionArray)
        upsertEntities.setObject(2, version)
        upsertEntities.setObject(3, version)
        upsertEntities.setObject(4, entitySetId)
        upsertEntities.setArray(5, entityKeyIdsArr)
        upsertEntities.setInt(6, partition)
        upsertEntities.setInt(7, partition)
        upsertEntities.setLong(8, version)


        val updatedLinkedEntities = upsertEntities.executeUpdate()
        logger.debug("Updated $updatedLinkedEntities linked entities as part of insert.")
        return updatedLinkedEntities
    }

    private fun getPropertyHash(
            entitySetId: UUID,
            entityKeyId: UUID,
            propertyTypeId: UUID,
            value: Any,
            dataType: EdmPrimitiveTypeKind
    ): Pair<ByteArray, Any> {
        return PostgresDataHasher.hashObject(value, dataType) to value
    }
}

private fun abortInsert(entitySetId: UUID, entityKeyId: UUID): Nothing {
    throw InvalidParameterException(
            "Cannot insert property type not in authorized property types for entity $entityKeyId from entity set $entitySetId."
    )
}
