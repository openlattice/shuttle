package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
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

    fun loadCargo(flightName: String) {
        val integration = integrations.getValue(flightName)

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

    fun createIntegration(flightName: String, integration: Integration) {
        checkState( !integrations.containsKey(flightName), "An integration with name $flightName already exists." )
        integrations[flightName] = integration
    }

    fun updateIntegration(flightName: String, integration: Integration) {
        checkState( integrations.containsKey(flightName), "Integration with name $flightName does not exist." )
        integrations[flightName] = integration
    }

    fun deleteIntegration(flightName: String) {
        checkState( integrations.containsKey(flightName), "Integration with name $flightName does not exist." )
        integrations[flightName] = null
    }

    private fun getDataSource(properties: Properties): HikariDataSource {
        val hikariConfig = HikariConfig(properties)
        return HikariDataSource(hikariConfig)
    }

}