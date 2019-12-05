package com.openlattice.shuttle

import com.google.common.base.Preconditions.checkState
import com.hazelcast.core.HazelcastInstance
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception
import java.util.*
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val fetchSize = 10000
private val readRateLimit = 1000
private val uploadBatchSize = 10000

@Inject
private lateinit var missionParameters: MissionParameters

@Service
class RecurringIntegrationService(
        private val hazelcastInstance: HazelcastInstance
) {

    private val integrations = hazelcastInstance.getMap<String, Integration>(HazelcastMap.INTEGRATIONS.name)

    fun loadCargo(integrationName: String) {
        val integration = integrations.getValue(integrationName)

        val dataSource = getDataSource(integration.source)
        val payload = JdbcPayload(readRateLimit.toDouble(), dataSource, integration.sql, fetchSize, readRateLimit != 0)
        val flightPlan = mapOf(integration.flight to payload)

        try {
            val config = missionParameters.postgres.config
            val missionControl = MissionControl(
                    integration.environment,
                    config.getProperty("username"),
                    config.getProperty("password"),
                    integration.s3bucket,
                    missionParameters)
            logger.info("Preparing flight plan.")
            val shuttle = missionControl.prepare(flightPlan,
                    false,
                    integration.sourcePrimaryKeyColumns,
                    integration.contacts)
            logger.info("Pre-flight check list complete.")
            shuttle.launch(uploadBatchSize)
            MissionControl.succeed()
        } catch (ex: Exception) {
            MissionControl.fail(1, integration.flight, ex)
        }
    }

    fun createIntegration(integrationName: String, integration: Integration) {
        checkState( !integrations.containsKey(integrationName), "An integration with name $integrationName already exists." )
        integrations[integrationName] = integration
    }

    fun updateIntegration(integrationName: String, integration: Integration) {
        checkState( integrations.containsKey(integrationName), "Integration with name $integrationName does not exist." )
        integrations[integrationName] = integration
    }

    fun deleteIntegration(integrationName: String) {
        checkState( integrations.containsKey(integrationName), "Integration with name $integrationName does not exist." )
        integrations[integrationName] = null
    }

    private fun getDataSource(properties: Properties): HikariDataSource {
        val hikariConfig = HikariConfig(properties)
        return HikariDataSource(hikariConfig)
    }

}