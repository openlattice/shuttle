package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.openlattice.client.ApiClient
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.S3Api
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.integration.destinations.PostgresDestination
import com.openlattice.data.integration.destinations.PostgresS3Destination
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.retrofit.RhizomeByteConverterFactory
import com.openlattice.retrofit.RhizomeCallAdapterFactory
import com.openlattice.retrofit.RhizomeJacksonConverterFactory
import com.openlattice.rhizome.proxy.RetrofitBuilders
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.hazelcast.processors.ReloadFlightEntryProcessor
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import retrofit2.Retrofit
import java.lang.Exception
import java.util.*

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val fetchSize = 10000
private val readRateLimit = 1000
private val uploadBatchSize = 10000

@Service
class RecurringIntegrationService(
        private val hazelcastInstance: HazelcastInstance,
        private val missionParameters: MissionParameters
) {

    private val integrations = hazelcastInstance.getMap<String, Integration>(HazelcastMap.INTEGRATIONS.name)
    private val entitySets = hazelcastInstance.getMap<UUID, EntitySet>(HazelcastMap.ENTITY_SETS.name)
    private val entityTypes = hazelcastInstance.getMap<UUID, EntityType>(HazelcastMap.ENTITY_TYPES.name)
    private val propertyTypes = hazelcastInstance.getMap<UUID, PropertyType>(HazelcastMap.PROPERTY_TYPES.name)
    private val creds = missionParameters.auth.credentials

    fun loadCargo(integrationName: String) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist")
        val integration = integrations.getValue(integrationName)

        val authToken = MissionControl.getIdToken(creds.getProperty("email"), creds.getProperty("password"), integration.environment)
        val apiClient = ApiClient(integration.environment) { authToken }
        val dataIntegrationApi = apiClient.dataIntegrationApi

        val srcDataSource = getSrcDataSource(integration.source)
        val payload = JdbcPayload(readRateLimit.toDouble(), srcDataSource, integration.sql, fetchSize, readRateLimit != 0)
        val flightPlan = mapOf(integration.flight!! to payload)
        val destinationsMap = generateDestinationsMap(integration, missionParameters, dataIntegrationApi)

        try {
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
                    StorageDestination.S3
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
    }

    fun readIntegrationDefinition(integrationName: String): Integration {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        return integrations.getValue(integrationName)
    }

    fun updateIntegrationDefinition(integrationName: String, integration: Integration) {
        checkState(integrations.containsKey(integrationName), "Integration with name $integrationName does not exist.")
        integrations[integrationName] = integration
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