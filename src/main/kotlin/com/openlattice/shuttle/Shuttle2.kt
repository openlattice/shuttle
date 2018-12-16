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

import com.dataloom.mappers.ObjectMappers
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.Sets
import com.openlattice.client.ApiClient
import com.openlattice.client.ApiFactoryFactory
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.edm.EdmApi
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.serialization.JacksonLambdaDeserializer
import com.openlattice.shuttle.serialization.JacksonLambdaSerializer
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */

class Shuttle2(
        authToken: String,
        environment: RetrofitFactory.Environment = RetrofitFactory.Environment.LOCAL
) {
    private val apiClient = ApiClient(environment) { authToken }
    private val edmApi = apiClient.edmApi

    private val storageDestByProperty = ConcurrentHashMap<UUID, StorageDestination>()
    private val seenEntitySetIds = Sets.newHashSet<UUID>()

    private val logger = LoggerFactory
            .getLogger(Shuttle::class.java)

    private val UPLOAD_BATCH_SIZE = 100000

    private val entitySetIdCache: LoadingCache<String, UUID> = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .build(object : CacheLoader<String, UUID>() {
                @Throws(Exception::class)
                override fun load(entitySetName: String): UUID {
                    return edmApi.getEntitySetId(entitySetName)
                }
            })


    private val propertyIdsCache: LoadingCache<FullQualifiedName, UUID>? = null

    private var keyCache: LoadingCache<String, LinkedHashSet<FullQualifiedName>> = CacheBuilder
    .newBuilder()
    .maximumSize(1000)
    .build<String, LinkedHashSet<FullQualifiedName>>(
    object : CacheLoader<String, LinkedHashSet<FullQualifiedName>>() {
        @Throws(Exception::class)
        override fun load(entitySetName: String): LinkedHashSet<FullQualifiedName> {
            return edmApi.getEntityType(edmApi.getEntitySet(edmApi.getEntitySetId(entitySetName)).entityTypeId
            )
                    .getKey().stream()
                    .map({ propertyTypeId -> edmApi.getPropertyType(propertyTypeId).getType() })
                    .collect(
                            Collectors.toCollection<FullQualifiedName, LinkedHashSet<FullQualifiedName>>(
                                    Supplier<LinkedHashSet<FullQualifiedName>> { LinkedHashSet() })
                    )
        }
    })


    @Throws(InterruptedException::class)
    fun launchPayloadFlight(flightsToPayloads: Map<Flight, Payload>) {
        ensureValidIntegration(flightsToPayloads)

    }

    private fun ensureValidIntegration(flightsToPayloads: Map<Flight, Payload>) {
        flightsToPayloads.keys.forEach { flight ->

            flight
                    .entities
                    .forEach { entityDefinition ->
                        val entitySetId = entitySetIdCache.getUnchecked(entityDefinition.getEntitySetName())
                        assertPropertiesMatchEdm(
                                entityDefinition.getEntitySetName(),
                                entitySetId,
                                entityDefinition.getProperties(),
                                edmApi
                        )
                    }

            flight
                    .associations
                    .forEach { associationDefinition ->
                        val entitySetId = entitySetIdCache.getUnchecked(associationDefinition.getEntitySetName())
                        assertPropertiesMatchEdm(
                                associationDefinition.getEntitySetName(),
                                entitySetId,
                                associationDefinition.getProperties(),
                                edmApi
                        )
                    }
        }
    }

    private fun assertPropertiesMatchEdm(
            entitySetName: String,
            entitySetId: UUID,
            properties: Collection<PropertyDefinition>,
            edmApi: EdmApi
    ) {

        val propertyFqns = properties.map(PropertyDefinition::getFullQualifiedName).toSet()
        val entitySetPropertyFqns = edmApi
                .getEntityType(edmApi.getEntitySet(entitySetId).entityTypeId).properties
                .map { id -> edmApi.getPropertyType(id).type }
                .toSet()
        val illegalFqns = Sets.filter(propertyFqns) { fqn -> !entitySetPropertyFqns.contains(fqn) }
        if (!illegalFqns.isEmpty()) {
            illegalFqns.forEach { fqn ->
                logger.error(
                        "Entity set {} does not contain any property type with FQN: {}",
                        entitySetName,
                        fqn.toString()
                )
            }
            throw NullPointerException("Illegal property types defined for entity set $entitySetName")
        }
    }

}