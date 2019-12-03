package com.openlattice.shuttle

import com.openlattice.data.DataGraphService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.postgres.PostgresColumn.NAME
import com.openlattice.postgres.PostgresTable.FLIGHTS
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CREATE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.READ_RATE_LIMIT

import com.openlattice.shuttle.ShuttleCliOptions.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.control.IntegrationResultSetAdapter
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val fetchSize = 10000
private val readRateLimit = 1000
private val uploadBatchSize = 10000

@Service
class RecurringIntegrationService(
        private val hds: HikariDataSource
) {

    fun loadCargo(flightName: String, lastRow: String) {
        val integration = hds.connection.use{
            val stmt = it.createStatement()
            val getIntegrationConfigSql = "SELECT * FROM ${FLIGHTS.name} WHERE ${NAME.name} = $flightName"
            val result = stmt.executeQuery(getIntegrationConfigSql)
            return@use IntegrationResultSetAdapter.integration(result)

        }

        //GET FLIGHTPLAN
        val payload = JdbcPayload(readRateLimit.toDouble(), hds, integration.sql, fetchSize, readRateLimit != 0)
        val flightPlan = mapOf(integration.flight to payload)

        try {
            val missionControl = MissionControl(
                    integration.environment,
                    username,
                    password,
                    integration.s3bucket,
                    MissionParameters.empty())
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

}