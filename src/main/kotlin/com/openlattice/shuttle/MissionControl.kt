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

import com.auth0.client.auth.AuthAPI
import com.auth0.exception.Auth0Exception
import com.google.common.annotations.VisibleForTesting
import com.openlattice.client.ApiClient
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.integration.destinations.PostgresDestination
import com.openlattice.data.integration.destinations.RestDestination
import com.openlattice.data.integration.destinations.S3Destination
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.rhizome.proxy.RetrofitBuilders
import com.openlattice.shuttle.payload.Payload
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.lang.UnsupportedOperationException
import java.util.*
import org.bouncycastle.crypto.tls.ConnectionEnd.client
import com.openlattice.retrofit.RhizomeCallAdapterFactory
import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Suppliers
import com.openlattice.data.S3Api
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.retrofit.RhizomeJacksonConverterFactory
import com.openlattice.retrofit.RhizomeByteConverterFactory
import retrofit2.Retrofit
import java.util.concurrent.TimeUnit
import java.util.function.Supplier


private const val AUTH0_CLIENT_ID = "o8Y2U2zb5Iwo01jdxMN1W2aiN8PxwVjh"
private const val AUTH0_CLIENT_DOMAIN = "openlattice.auth0.com"
private const val AUTH0_CONNECTION = "Username-Password-Authentication"
private const val AUTH0_SCOPES = "openid email nickname roles user_id organizations"

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MissionControl(environment: RetrofitFactory.Environment, authToken: Supplier<String>, s3BucketUrl: String) {

    constructor(
            environment: RetrofitFactory.Environment, username: String, password: String, s3BucketUrl: String
    ) : this(
            environment,
            Suppliers.memoizeWithExpiration({ getIdToken(username, password) }, 1, TimeUnit.HOURS),
            s3BucketUrl
    )

    companion object {
        private val logger = LoggerFactory.getLogger(MissionControl::class.java)
        private val client = MissionControl.buildClient(AUTH0_CLIENT_ID)

        @JvmStatic
        @Throws(Auth0Exception::class)
        fun getIdToken(username: String, password: String): String {
            return getIdToken(client, AUTH0_CONNECTION, username, password)
        }

        @JvmStatic
        @VisibleForTesting
        fun buildClient(clientId: String): AuthAPI {
            return AuthAPI(AUTH0_CLIENT_DOMAIN, clientId, "")
        }

        @JvmStatic
        @VisibleForTesting
        @Throws(Auth0Exception::class)
        fun getIdToken(auth0: AuthAPI, realm: String, username: String, password: String): String {
            return auth0
                    .login(username, password, realm)
                    .setScope(AUTH0_SCOPES)
                    .setAudience("https://api.openlattice.com")
                    .execute()
                    .idToken
        }
    }

    //TODO: We need to figure out why this isn't registered or a better way of controlling watch ObjectMapper is used
    //by retrofit clients.
    init {
        FullQualifiedNameJacksonSerializer.registerWithMapper(ObjectMappers.getJsonMapper())
    }

    private val apiClient = ApiClient(environment) { authToken.get() }
    private val edmApi = apiClient.edmApi
    private val dataApi = apiClient.dataApi
    private val dataIntegrationApi = apiClient.dataIntegrationApi

    private val s3Api = Retrofit.Builder()
            .baseUrl(s3BucketUrl)
            .addConverterFactory(RhizomeByteConverterFactory())
            .addConverterFactory(RhizomeJacksonConverterFactory(ObjectMappers.getJsonMapper()))
            .addCallAdapterFactory(RhizomeCallAdapterFactory())
            .client(RetrofitBuilders.okHttpClient().build())
            .build().create(S3Api::class.java)

    private val entitySets = edmApi.entitySets.map { it.name to it }.toMap().toMutableMap()
    private val entityTypes = edmApi.entityTypes.map { it.id to it }.toMap().toMutableMap()
    private val propertyTypes = edmApi.propertyTypes.map { it.type to it }.toMap().toMutableMap()
    private val propertyTypesById = propertyTypes.values.map { it.id to it }.toMap().toMutableMap()
    private val integrationDestinations = mapOf(
            StorageDestination.REST to RestDestination(dataApi),
            StorageDestination.POSTGRES to PostgresDestination(),
            StorageDestination.S3 to S3Destination(dataApi, s3Api, dataIntegrationApi)
    )


    fun prepare(
            flightPlan: Map<Flight, Payload>,
            createEntitySets: Boolean = false,
            contacts: Set<String> = setOf()
    ): Shuttle {
        if (createEntitySets) {
            createMissingEntitySets(flightPlan, contacts)
        }
        ensureValidIntegration(flightPlan)
        return Shuttle(
                flightPlan,
                entitySets,
                entityTypes,
                propertyTypes,
                propertyTypesById,
                integrationDestinations,
                dataIntegrationApi
        )
    }

    private fun createMissingEntitySets(flightPlan: Map<Flight, Payload>, contacts: Set<String>) {
        flightPlan.keys.forEach {
            it.entities.forEach { entityDefinition ->
                if (entitySets.containsKey(entityDefinition.entitySetName)) {

                } else {
                    val fqn = entityDefinition.getEntityTypeFqn()
                    val entitySet = EntitySet(
                            entityDefinition.getId().orElse(UUID.randomUUID()),
                            edmApi.getEntityTypeId(fqn.namespace, fqn.name),
                            entityDefinition.getEntitySetName(),
                            entityDefinition.getEntitySetName(),
                            Optional.of(entityDefinition.getEntitySetName()),
                            contacts
                    )
                    val entitySetId = edmApi.createEntitySets(setOf(entitySet))[entityDefinition.entitySetName]!!
                    check(entitySetId == entitySet.id) { "Submitted entity set id does not match return." }
                    entitySets[entityDefinition.entitySetName] = entitySet
                }
            }
            it.associations.forEach { associationDefinition ->
                if (entitySets.containsKey(associationDefinition.entitySetName)) {

                } else {
                    val fqn = associationDefinition.getEntityTypeFqn()
                    val entityTypeId = edmApi.getEntityTypeId(fqn.namespace, fqn.name)
                    val entitySet = EntitySet(
                            associationDefinition.getId().orElse(UUID.randomUUID()),
                            entityTypeId,
                            associationDefinition.getEntitySetName(),
                            associationDefinition.getEntitySetName(),
                            Optional.of(associationDefinition.getEntitySetName()),
                            contacts
                    )
                    val entitySetId = edmApi.createEntitySets(
                            setOf(entitySet)
                    )[associationDefinition.entitySetName]!!
                    check(entitySetId == entitySet.id) { "Submitted entity set id does not match return." }
                    entitySets[associationDefinition.entitySetName] = entitySet
                }
            }
        }
    }


    private fun ensureValidIntegration(flight: Map<Flight, Payload>) {
        flight.keys.forEach { flight ->

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
        check(entitySets.contains(entitySetName)) { "Entity set $entitySetName does not exist." }
        val missingProperties = properties.filter { !propertyTypes.contains(it.fullQualifiedName) }
        if (missingProperties.isNotEmpty()) {
            missingProperties.forEach {
                logger.error("The fqn ${it.fullQualifiedName}is not associated with any property type ")
            }
            throw NoSuchElementException("The following property definition fqn were not found: $missingProperties")
        }

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

