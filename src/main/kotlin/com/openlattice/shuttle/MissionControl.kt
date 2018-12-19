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
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.payload.Payload
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*


private const val AUTH0_CLIENT_ID = "o8Y2U2zb5Iwo01jdxMN1W2aiN8PxwVjh"
private const val AUTH0_CLIENT_DOMAIN = "openlattice.auth0.com"
private const val AUTH0_CONNECTION = "Username-Password-Authentication"
private const val AUTH0_SCOPES = "openid email nickname roles user_id organizations"
private val client = MissionControl.buildClient(AUTH0_CLIENT_ID)


/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class MissionControl(environment: RetrofitFactory.Environment, authToken: String, ) {

    companion object {
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

    private val apiClient = ApiClient(environment) { authToken }
    private val edmApi = apiClient.edmApi
    private val dataApi = apiClient.dataApi

    private val entitySets = edmApi.entitySets.map{ it.name to it}.toMap().toMutableMap()
    private val entityTypes = edmApi.entityTypes.map { it.id to it }.toMap().toMutableMap()
    private val propertyTypes = edmApi.propertyTypes.map{ it.datatype to it }.toMap().toMutableMap()
    private val propertyTypesById = propertyTypes.values.map { it.id to it }.toMap().toMutableMap()
    private val integrationDestinations = mapOf(
            StorageDestination.REST to RestDestination(dataApi),
            StorageDestination.POSTGRES to PostgresDestination(),
            StorageDestination.S3 to PostgresDestination()
    )
    private val logger = LoggerFactory.getLogger(MissionControl::class.java)

    fun startLaunch(
            flightPlan: Map<Flight, Payload>,
            createEntitySets: Boolean = false,
            contacts: Set<String> = setOf()
    ) {
        ensureValidIntegration(flightPlan)
        if( createEntitySets ) {
            createMissingEntitySets(flightPlan, contacts)
        }
    }

    private fun createMissingEntitySets(flightPlan: Map<Flight, Payload>, contacts: Set<String>) {
        val requiredEntitySets = flightPlan.keys.forEach {
            it.entities.forEach { entityDefinition ->
                if (entitySets.containsKey(entityDefinition.entitySetName)) {

                } else {
                    val entitySet = EntitySet(
                            entityDefinition.getId().orElse(UUID.randomUUID()),
                            edmApi.getEntityTypeId(entityDefinition.getEntityTypeFqn()),
                            entityDefinition.getEntitySetName(),
                            entityDefinition.getEntitySetName(),
                            Optional.of(entityDefinition.getEntitySetName()),
                            contacts
                    )
                    val entitySetId = edmApi.createEntitySets(setOf( entitySet ) )[entityDefinition.entitySetName]!!
                    check( entitySetId == entitySet.id ) { "Submitted entity set id does not match return." }
                    entitySets[entityDefinition.entitySetName ] = entitySet
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
        val requiredPropertyTypes = properties.map { propertyTypes[it.fullQualifiedName]!!.id }.toSet()
        val actualPropertyTypes = entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.properties

        val missingPropertyTypes = requiredPropertyTypes - actualPropertyTypes
        missingPropertyTypes.forEach {
            Shuttle2.logger.error(
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

