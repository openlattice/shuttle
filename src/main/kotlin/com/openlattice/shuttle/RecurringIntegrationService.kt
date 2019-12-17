package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.openlattice.client.ApiClient
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKeyIdService
import com.openlattice.data.S3Api
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.integration.destinations.PostgresDestination
import com.openlattice.data.integration.destinations.PostgresS3Destination
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.retrofit.RhizomeByteConverterFactory
import com.openlattice.retrofit.RhizomeCallAdapterFactory
import com.openlattice.retrofit.RhizomeJacksonConverterFactory
import com.openlattice.rhizome.proxy.RetrofitBuilders
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationKey
import com.openlattice.shuttle.control.IntegrationStatus
import com.openlattice.shuttle.control.IntegrationUpdate
import com.openlattice.shuttle.hazelcast.processors.ReloadFlightEntryProcessor
import com.openlattice.shuttle.hazelcast.processors.UpdateIntegrationEntryProcessor
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import retrofit2.Retrofit
import java.lang.Exception
import java.time.OffsetDateTime
import java.util.*

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val fetchSize = 10000
private val readRateLimit = 1000
private val uploadBatchSize = 10000

@Service
class RecurringIntegrationService(
        private val hazelcastInstance: HazelcastInstance,
        private val missionParameters: MissionParameters,
        private val hds: HikariDataSource,
        private val idService: EntityKeyIdService,
        private val entitySetManager: EntitySetManager
) {

    private val integrations = hazelcastInstance.getMap<String, Integration>(HazelcastMap.INTEGRATIONS.name)
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val propertyTypes = hazelcastInstance.getMap<UUID, PropertyType>(HazelcastMap.PROPERTY_TYPES.name)
    private val creds = missionParameters.auth.credentials
    private val shuttleLogsEntitySetId = UUID.fromString("fc8de069-0455-404f-9b0c-a8a06bc3b467") //TODO pipe this in properly


    fun loadCargo(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist")
        val integration = integrations.getValue(integrationName)

//        val authToken = MissionControl.getIdToken(creds.getProperty("email"), creds.getProperty("password"))
        val apiClient = ApiClient(integration.environment) { "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6InBpcGVyQG9wZW5sYXR0aWNlLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJ1c2VyX21ldGFkYXRhIjp7fSwiYXBwX21ldGFkYXRhIjp7InJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iXSwiYWN0aXZhdGVkIjoiYWN0aXZhdGVkIn0sIm5pY2tuYW1lIjoicGlwZXIiLCJyb2xlcyI6WyJBdXRoZW50aWNhdGVkVXNlciIsImFkbWluIl0sInVzZXJfaWQiOiJnb29nbGUtb2F1dGgyfDExNDczMDU3NjI0MjQ4MTc3NTE4MiIsImlzcyI6Imh0dHBzOi8vb3BlbmxhdHRpY2UuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTE0NzMwNTc2MjQyNDgxNzc1MTgyIiwiYXVkIjoiS1R6Z3l4czZLQmNKSEI4NzJlU01lMmNwVEh6aHhTOTkiLCJpYXQiOjE1NzY1MzgwODksImV4cCI6MTU3NjYyNDQ4OX0.6goFLA_-amOW53j9WCV_CZeJsT1bfIZNoe7G4fRkkpU" }
        val dataIntegrationApi = apiClient.dataIntegrationApi

        val srcDataSource = getSrcDataSource(integration.source)
        val payload = JdbcPayload(readRateLimit.toDouble(), srcDataSource, integration.sql, fetchSize, readRateLimit != 0)
        val flightPlan = mapOf(integration.flight!! to payload)
        val destinationsMap = generateDestinationsMap(integration, missionParameters, dataIntegrationApi)


        try {
            val shuttleLogsEntitySet = entitySets.getValue(shuttleLogsEntitySetId)
            val shuttleLogsEntityType = entityTypes.getValue(shuttleLogsEntitySet.entityTypeId)
            val shuttleLogsProperties = shuttleLogsEntityType.properties.map{
                it to propertyTypes.getValue(it)
            }.toMap()
            val shuttleLogsDestination = PostgresDestination(
                    mapOf(shuttleLogsEntitySetId to shuttleLogsEntitySet),
                    mapOf(shuttleLogsEntitySet.entityTypeId to shuttleLogsEntityType),
                    shuttleLogsProperties,
                    hds
            )

            val shuttle = Shuttle(
                    integration.environment,
                    flightPlan,
                    entitySets.values.associateBy { it.name },
                    entityTypes.values.associateBy { it.id },
                    propertyTypes.values.associateBy { it.type },
                    destinationsMap,
                    dataIntegrationApi,
                    integration.sourcePrimaryKeyColumns,
                    missionParameters,
                    StorageDestination.S3,
                    true,
                    shuttleLogsDestination,
                    shuttleLogsProperties.values.associateBy{it.type},
                    idService
            )
            shuttle.launch(uploadBatchSize)
            logger.info("YOU DID IT :D, integration with name $integrationName completed.")
        } catch (ex: Exception) {
            logger.info("You did not do it :'(, integration with name $integrationName failed with exception $ex")
        }
    }

    fun createIntegrationDefinition(integrationName: String, integration: Integration) {
        checkState(!integrations.containsKey(integrationName), "An integration with name $integrationName already exists.")
        integrations[integrationName] = integration
        entitySetManager.createEntitySet()

    }

    fun readIntegrationDefinition(integrationName: String): Integration {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        return integrations.getValue(integrationName)
    }

    fun updateIntegrationDefinition(integrationName: String, integrationUpdate: IntegrationUpdate) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        integrations.executeOnKey(integrationName, UpdateIntegrationEntryProcessor(integrationUpdate))
    }

    fun updateFlightWithinIntegrationDefinition(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        integrations.executeOnKey(integrationName, ReloadFlightEntryProcessor())
    }

    fun deleteIntegrationDefinition(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        integrations[integrationName] = null
    }

    private fun getSrcDataSource(source: Properties): HikariDataSource {
        return HikariDataSource(HikariConfig(source))
    }

    private fun generateDestinationsMap(integration: Integration, missionParameters: MissionParameters, dataIntegrationApi: DataIntegrationApi): Map<StorageDestination, IntegrationDestination> {
        val s3BucketUrl = integration.s3bucket

        val pgDestination = PostgresDestination(
                entitySets.mapKeys { it.value.id },
                entityTypes,
                propertyTypes.mapKeys { it.value.id },
                HikariDataSource(HikariConfig(missionParameters.postgres.config))
        )

        if (s3BucketUrl.isNotBlank()) {
            val s3Api = Retrofit.Builder()
                    .baseUrl(s3BucketUrl)
                    .addConverterFactory(RhizomeByteConverterFactory())
                    .addConverterFactory(RhizomeJacksonConverterFactory(ObjectMappers.getJsonMapper()))
                    .addCallAdapterFactory(RhizomeCallAdapterFactory())
                    .client(RetrofitBuilders.okHttpClient().build())
                    .build().create(S3Api::class.java)
            val s3Destination = PostgresS3Destination(pgDestination, s3Api, dataIntegrationApi)
            return mapOf(StorageDestination.POSTGRES to pgDestination, StorageDestination.S3 to s3Destination)
        }
        return mapOf(StorageDestination.POSTGRES to pgDestination)
    }

}