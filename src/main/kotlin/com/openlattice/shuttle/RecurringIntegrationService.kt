package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.openlattice.admin.SQL
import com.openlattice.client.RetrofitFactory
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CREATE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.READ_RATE_LIMIT
import com.openlattice.shuttle.ShuttleCliOptions.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)

@Service
class RecurringIntegrationService(
        private val hds: HikariDataSource
) {

    fun loadCargo(flightName: String, lastRow: String) {
        //TODO get flight and other vals from entity set

        val env = RetrofitFactory.Environment.PRODUCTION

        //GET FLIGHTPLAN
        val flightAsString = "an input yaml as string, gotten using flightName"
        val flight = ObjectMappers.getYamlMapper().readValue(flightAsString, Flight::class.java)
        val args = arrayOf("i'll", "get", "you", "later") //gotten from entity set
        val cl = ShuttleCliOptions.parseCommandLine(args)
        val sql = cl.getOptionValue(SQL)
        val rRL = cl.getOptionValue(READ_RATE_LIMIT).toInt()
        val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
        val payload = JdbcPayload(rRL.toDouble(), hds, sql, fetchSize, rRL != 0)
        val flightPlan = mapOf(flight to payload)

        //GET CONTACT
        val contacts = setOf("get", "from", "es")

        //GET PKEY COLS
        val pkeyCols = listOf("get", "from", "es")

        //GET UPLOAD BATCH SIZE
        val uploadBatchSize = if (cl.hasOption(UPLOAD_SIZE)) {
            cl.getOptionValue(UPLOAD_SIZE).toInt()
        } else {
            DEFAULT_UPLOAD_SIZE
        }

        try {
            val missionControl = MissionControl(env, "username", "password", "bucket", MissionParameters.empty())
            logger.info("Preparing flight plan.")
            val shuttle = missionControl.prepare(flightPlan, cl.hasOption(CREATE), pkeyCols, contacts)
            logger.info("Pre-flight check list complete.")
            shuttle.launch(uploadBatchSize)
            MissionControl.succeed()
        } catch (ex: Exception) {
            MissionControl.fail(1, flight, ex)
        }
    }

}