package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.openlattice.IdConstants
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataGraphService
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.shuttle.ShuttleCliOptions.Companion.CREATE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCliOptions.Companion.READ_RATE_LIMIT

import com.openlattice.shuttle.ShuttleCliOptions.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.payload.JdbcPayload
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.cli.CommandLine
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import java.lang.Exception
import java.util.*
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val flightEntitySetId = IdConstants.FLIGHT_ENTITY_SET_ID.id

@Inject
private lateinit var datastoreConfig: DatastoreConfiguration

@Service
class RecurringIntegrationService(
        private val hds: HikariDataSource,
        private val entitySetManager: EntitySetService,
        private val dataGraphService: DataGraphService
) {

    fun loadCargo(flightName: String, lastRow: String) {
        val flightProperties = entitySetManager.getPropertyTypesForEntitySet(flightEntitySetId)
        val flightEntity = dataGraphService.getEntity(
                flightEntitySetId,
                entityKeyId,
                flightProperties)

        //PARSE FLIGHT
        val fqns = flightConfig.fqns.map { it.key to FullQualifiedName(it.value) }.toMap()
        val flightName = flightEntity.getValue(fqns[NAME]!!).first().toString()
        val credentials = flightConfig.credentials.getValue(flightName)
        val username = credentials.getValue("username")
        val password = credentials.getValue("password")
        val flightAsString = flightEntity.getValue(fqns[DEFINITION]!!).first().toString()
        val flight = ObjectMappers.getYamlMapper().readValue(flightAsString, Flight::class.java)
        val sql = flightEntity.getValue(fqns[SQL]!!).first().toString()
        val env = flightEntity.getValue(fqns[ENVIRONMENT]!!).first().toString().toUpperCase()
        val retrofitEnv = RetrofitFactory.Environment.valueOf(env)

        //GET FLIGHTPLAN
        val args = flightEntity.getValue(fqns[ARGS]!!).map { it.toString() }.toTypedArray()
        val cl = ShuttleCliOptions.parseCommandLine(args)
        val payload = getPayload(cl, sql)
        val flightPlan = mapOf(flight to payload)

        //GET CONTACT
        val contacts = flightEntity.getValue(fqns[CONTACT]!!).map { it.toString() }.toSet()

        //GET PKEY COLS
        val pkeyCols = flightEntity.getValue(fqns[PKEY]!!).map { it.toString() }

        //GET UPLOAD BATCH SIZE
        val uploadBatchSize = if (cl.hasOption(UPLOAD_SIZE)) {
            cl.getOptionValue(UPLOAD_SIZE).toInt()
        } else {
            DEFAULT_UPLOAD_SIZE
        }

        try {
            val s3BucketUrl = "https://" + datastoreConfig.bucketName + ".s3-website-us-gov-west-1.amazonaws.com"
            val missionControl = MissionControl(retrofitEnv, username, password, s3BucketUrl, MissionParameters.empty())
            logger.info("Preparing flight plan.")
            val shuttle = missionControl.prepare(flightPlan, cl.hasOption(CREATE), pkeyCols, contacts)
            logger.info("Pre-flight check list complete.")
            shuttle.launch(uploadBatchSize)
            MissionControl.succeed()
        } catch (ex: Exception) {
            MissionControl.fail(1, flight, ex)
        }
    }

    private fun getPayload(cl: CommandLine, sql: String): JdbcPayload {
        val rRL = cl.getOptionValue(READ_RATE_LIMIT).toInt()
        val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
        return JdbcPayload(rRL.toDouble(), hds, sql, fetchSize, rRL != 0)
    }

}