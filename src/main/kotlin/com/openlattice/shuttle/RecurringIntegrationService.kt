package com.openlattice.shuttle

import com.dataloom.mappers.ObjectMappers
import com.openlattice.IdConstants
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataGraphService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.datastore.configuration.DatastoreConfiguration
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.shuttle.FlightProperty.*
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
import java.util.function.Supplier
import javax.inject.Inject

private val logger = LoggerFactory.getLogger(RecurringIntegrationService::class.java)
private val flightEntitySetId = IdConstants.FLIGHT_ENTITY_SET_ID.id

@Inject
private lateinit var datastoreConfig: DatastoreConfiguration

@Service
class RecurringIntegrationService(
        private val hds: HikariDataSource,
        private val entitySetManager: EntitySetService,
        private val dataGraphService: DataGraphService,
        private val flightConfig: FlightConfiguration
) {

    fun loadCargo(entityKeyId: UUID, lastRow: String) {
        val env = RetrofitFactory.Environment.PRODUCTION //?????
        val flightProperties = entitySetManager.getPropertyTypesForEntitySet(flightEntitySetId)
        val flightEntity = dataGraphService.getEntity(
                flightEntitySetId,
                entityKeyId,
                flightProperties)

        //GET FLIGHTPLAN
        val fqns = flightConfig.fqns.map { it.key to FullQualifiedName(it.value) }.toMap()
        val flightAsString = flightEntity.getValue(fqns[DEFINITION]!!).first().toString()
        val flight = ObjectMappers.getYamlMapper().readValue(flightAsString, Flight::class.java)
        val sql = flightEntity.getValue(fqns[SQL]!!).first().toString()
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

        //TODO generate bucket from profiles?!?!???!

        try {
            val s3BucketUrl = "https://" + datastoreConfig.bucketName + ".s3-website-us-gov-west-1.amazonaws.com"
            val missionControl = MissionControl(env, Supplier{ "" }, s3BucketUrl, MissionParameters.empty())
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