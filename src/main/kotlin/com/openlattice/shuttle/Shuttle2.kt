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

package com.openlattice.shuttle

import com.openlattice.ApiUtil
import com.openlattice.ApiUtil.generateDefaultEntityId
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.payload.Payload
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.stream.Stream


const val UPLOAD_BATCH_SIZE = 100000

/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */
class Shuttle2(
        private val flightPlan: Map<Flight, Payload>,
        private val entitySets: Map<String, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<FullQualifiedName, PropertyType>,
        private val propertyTypesById: Map<UUID, PropertyType>,
        private val integrationDestinations: Map<StorageDestination, IntegrationDestination>,
        private val dataIntegrationApi: DataIntegrationApi,
        authToken: String,
        environment: RetrofitFactory.Environment = RetrofitFactory.Environment.LOCAL
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Shuttle::class.java)
    }
//    private val apiClient = ApiClient(environment) { authToken }
//    private val edmApi = apiClient.edmApi
//    private val dataApi = apiClient.dataApi

    private val storageDestByProperty = ConcurrentHashMap<StorageDestination, ConcurrentMap<UUID, StorageDestination>>()


    @Throws(InterruptedException::class)
    fun launchPayloadFlight(flightsToPayloads: Map<Flight, Payload>) {
        ensureValidIntegration(flightsToPayloads)
        flightsToPayloads.entries.forEach { entry ->

            logger.info("Launching flight: {}", entry.key.name)
            launchFlight(entry.key, entry.value.payload)
            logger.info("Finished flight: {}", entry.key.name)

        }

        System.exit(0)

    }

    /**
     * This function works under the assumption that the set returned from key is a unmodifiable linked hash set.
     */
    private fun getKeys(entitySetName: String): Set<UUID> {
        return entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.key
    }

    /**
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     *
     * @param key A stable set of ordered primary key property type ids to use for default entity key generation.
     */
    private fun generateDefaultEntityId(
            key: Set<UUID>,
            properties: Map<UUID, Set<Any>>
    ): String {
        return ApiUtil.generateDefaultEntityId(key.stream(), properties)
    }

    fun launchFlight(flight: Flight, payload: Stream<Map<String, Any>>) {
        logger.info("Launching flight: {}", flight.name)
        val remaining = payload.parallel().map { row ->

            val aliasesToEntityKey = mutableMapOf<String, EntityKey>()
            val addressedDataHolder = AddressedDataHolder(mutableMapOf(), mutableMapOf())
//            val entities = mutableSetOf<Entity>()
//            val associations = mutableSetOf<Association>()
            val wasCreated = mutableMapOf<String, Boolean>()

            if (flight.condition.isPresent && !(flight.valueMapper.apply(row) as Boolean)) {
                return@map addressedDataHolder
                //BulkDataCreation(entities, associations)
            }

            for (entityDefinition in flight.entities) {
                val condition = if (entityDefinition.condition.isPresent) {
                    entityDefinition.valueMapper.apply(row) as Boolean
                } else {
                    true
                }

                val entitySetId = entitySets[entityDefinition.entitySetName]!!.id
                val properties = mutableMapOf<UUID, MutableSet<Any>>()
                val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()
                for (propertyDefinition in entityDefinition.properties) {
                    val propertyValue = propertyDefinition.propertyValue.apply(row)
                    if (propertyValue != null &&
                            ((propertyValue !is String) || (propertyValue is String) && propertyValue.isNotBlank())) {
                        val storageDestination = propertyDefinition.storageDestination.orElseGet {
                            when (propertyTypes[propertyDefinition.fullQualifiedName]!!.datatype) {
                                EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                else -> StorageDestination.REST
                            }
                        }

                        val propertyId = propertyTypes[propertyDefinition.fullQualifiedName]!!.id

                        addressedProperties
                                .getOrPut(storageDestination) { mutableMapOf() }
                                .getOrPut(propertyId) { mutableSetOf() }
                                .add(propertyValue)

                        properties.getOrPut(propertyId) { mutableSetOf() }.add(propertyValue)
                    }
                }

                /*
                 * For entityId generation to work correctly it is very important that Stream remain ordered.
                 * Ordered != sequential vs parallel.
                 */

                val entityId = entityDefinition.generator
                        .map { it.apply(row) }
                        .orElseGet {
                            generateDefaultEntityId(getKeys(entityDefinition.entitySetName), properties)
                        }

                if (StringUtils.isNotBlank(entityId) and condition and properties.isNotEmpty()) {
                    val key = EntityKey(entitySetId, entityId)
                    aliasesToEntityKey[entityDefinition.alias] = key
                    addressedProperties.forEach { storageDestination, properties ->
                        addressedDataHolder.entities
                                .getOrPut(storageDestination) { mutableSetOf() }
                                .add(Entity(key, properties))
                    }
//                    entities.add(Entity(key, properties))
                    wasCreated[entityDefinition.alias] = true
                } else {
                    wasCreated[entityDefinition.alias] = false
                }

                MissionControl.signal()
            }

            for (associationDefinition in flight.associations) {

                if (associationDefinition.condition.isPresent &&
                        !(associationDefinition.valueMapper.apply(row) as Boolean)) {
                    continue
                }

                if (!wasCreated.containsKey(associationDefinition.dstAlias)) {
                    logger.error(
                            "Destination " + associationDefinition.dstAlias
                                    + " cannot be found to construct association " + associationDefinition.alias
                    )
                }

                if (!wasCreated.containsKey(associationDefinition.srcAlias)) {
                    logger.error(
                            ("Source " + associationDefinition.srcAlias
                                    + " cannot be found to construct association " + associationDefinition.alias)
                    )
                }
                if ((wasCreated[associationDefinition.srcAlias]!! && wasCreated[associationDefinition.dstAlias]!!)) {

                    val entitySetId = entitySets[associationDefinition.entitySetName]!!.id
                    val properties = mutableMapOf<UUID, MutableSet<Any>>()
                    val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()

                    for (propertyDefinition in associationDefinition.properties) {
                        val propertyValue = propertyDefinition.propertyValue.apply(row)
                        if (propertyValue != null &&
                                ((propertyValue !is String) || (propertyValue is String) && propertyValue.isNotBlank())) {

                            val storageDestination = propertyDefinition.storageDestination.orElseGet {
                                when (propertyTypes[propertyDefinition.fullQualifiedName]!!.datatype) {
                                    EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                    else -> StorageDestination.REST
                                }
                            }

                            val propertyId = propertyTypes[propertyDefinition.fullQualifiedName]!!.id

                            addressedProperties
                                    .getOrPut(storageDestination) { mutableMapOf() }
                                    .getOrPut(propertyId) { mutableSetOf() }
                                    .add(propertyValue)

                            properties.getOrPut(propertyId) { mutableSetOf() }.add(propertyValue)
                        }
                    }

                    val entityId = associationDefinition.generator
                            .map { it.apply(row) }
                            .orElseGet {
                                generateDefaultEntityId(
                                        getKeys(associationDefinition.entitySetName).stream(),
                                        properties
                                )
                            }

                    if (StringUtils.isNotBlank(entityId)) {
                        val key = EntityKey(entitySetId, entityId)
                        val src = aliasesToEntityKey[associationDefinition.srcAlias]
                        val dst = aliasesToEntityKey[associationDefinition.dstAlias]
                        addressedProperties.forEach { storageDestination, properties ->
                            addressedDataHolder.associations
                                    .getOrPut(storageDestination) { mutableSetOf() }
                                    .add(Association(key, src, dst, properties))

                        }
//                        associations.add(Association(key, src, dst, properties))
                    } else {
                        logger.error(
                                "Encountered blank entity id for entity set {}",
                                associationDefinition.entitySetName
                        )
                    }
                }

                MissionControl.signal()
            }
            addressedDataHolder
        }.reduce { a: AddressedDataHolder, b: AddressedDataHolder ->
            b.associations.forEach { storageDestination, associations ->
                a.associations.getOrPut(storageDestination) { mutableSetOf() }.addAll(associations)
            }

            b.entities.forEach { storageDestination, entities ->
                a.entities.getOrPut(storageDestination) { mutableSetOf() }.addAll(entities)
            }

            if (a.associations.values.any { it.size > UPLOAD_BATCH_SIZE } ||
                    a.entities.values.any { it.size > UPLOAD_BATCH_SIZE }) {
                val entityKeys = (a.entities.flatMap { it.value.map { it.key } }
                        + a.associations.flatMap { it.value.map { it.key } })
                val entityKeyIds = entityKeys.zip(dataIntegrationApi.getEntityKeyIds(entityKeys)).toMap()

                integrationDestinations.forEach {
                    it.value.integrateEntities(a.entities[it.key]!!, entityKeyIds)
                    it.value.integrateAssociations(a.associations[it.key]!!, entityKeyIds)
                }

                return@reduce AddressedDataHolder(mutableMapOf(), mutableMapOf())
            }
            a
        }

        remaining.ifPresent { r ->
            val entityKeys = r.entities.flatMap { it.value.map { it.key } } + r.associations.flatMap { it.value.map { it.key } }
            val entityKeyIds = entityKeys.zip(dataIntegrationApi.getEntityKeyIds(entityKeys)).toMap()
            integrationDestinations.forEach {
                it.value.integrateEntities(r.entities[it.key]!!, entityKeyIds)
                it.value.integrateAssociations(r.associations[it.key]!!, entityKeyIds)
            }
        }
    }

    private fun ensureValidIntegration(flightsToPayloads: Map<Flight, Payload>) {
        flightsToPayloads.keys.forEach { flight ->

            flight
                    .entities
                    .forEach { entityDefinition ->
                        assertPropertiesMatchEdm(entityDefinition.entitySetName, entityDefinition.properties)
                    }

            flight
                    .associations
                    .forEach { associationDefinition ->
                        assertPropertiesMatchEdm(associationDefinition.entitySetName, associationDefinition.properties)
                    }
        }
    }

    private fun assertPropertiesMatchEdm(entitySetName: String, properties: Collection<PropertyDefinition>) {
        val requiredPropertyTypes = properties.map { propertyTypes[it.fullQualifiedName]!!.id }.toSet()
        val actualPropertyTypes = entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.properties

        val missingPropertyTypes = requiredPropertyTypes - actualPropertyTypes
        missingPropertyTypes.forEach {
            logger.error(
                    "Entity set {} does not contain any property type with FQN: {}",
                    entitySetName,
                    propertyTypesById[it]!!.type
            )
            throw IllegalStateException(
                    "Property types $missingPropertyTypes not defined for entity set $entitySetName"
            )
        }
    }
}