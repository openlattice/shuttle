package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.openlattice.IdConstants
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataGraphService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.shuttle.FlightFqnConstants.*
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CREATE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.READ_RATE_LIMIT

import com.openlattice.shuttle.ShuttleCliOptions.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception
import java.util.*

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val flightEntitySetId = IdConstants.FLIGHT_ENTITY_SET_ID.id
@Service
class RecurringIntegrationService(
        private val hds: HikariDataSource,
        private val entitySetManager: EntitySetService,
        private val dataGraphService: DataGraphService
) {

    fun loadCargo(entityKeyId: UUID, lastRow: String) {
        //TODO get flight and other vals from entity set

        val env = RetrofitFactory.Environment.PRODUCTION //?????
        val flightProperties = entitySetManager.getPropertyTypesForEntitySet(flightEntitySetId)
        val flightEntity = dataGraphService.getEntity(
                flightEntitySetId,
                entityKeyId,
                flightProperties)

        //GET FLIGHTPLAN
        val flightAsString = flightEntity.getValue(DEFINITION.fqn).first() as String
        val flight = ObjectMappers.getYamlMapper().readValue(flightAsString, Flight::class.java)
        val sql = flightEntity.getValue(SQL.fqn).first() as String
        val args = flightEntity.getValue(ARGS.fqn).map { it as String }.toTypedArray()
        val cl = ShuttleCliOptions.parseCommandLine(args)
        val rRL = cl.getOptionValue(READ_RATE_LIMIT).toInt()
        val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
        val payload = JdbcPayload(rRL.toDouble(), hds, sql, fetchSize, rRL != 0)
        val flightPlan = mapOf(flight to payload)

        //GET CONTACT
        val contacts = flightEntity.getValue(CONTACT.fqn).map { it as String }.toSet()

        //GET PKEY COLS
        val pkeyCols = flightEntity.getValue(PKEY.fqn).map{it as String}

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