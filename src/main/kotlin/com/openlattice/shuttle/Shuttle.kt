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

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Slf4jReporter
import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions
import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableList
import com.google.common.collect.Queues
import com.openlattice.ApiUtil
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.ShuttleCli.Companion.CONFIGURATION
import com.openlattice.shuttle.ShuttleCli.Companion.CREATE
import com.openlattice.shuttle.ShuttleCli.Companion.CSV
import com.openlattice.shuttle.ShuttleCli.Companion.DATASOURCE
import com.openlattice.shuttle.ShuttleCli.Companion.DATA_ORIGIN
import com.openlattice.shuttle.ShuttleCli.Companion.ENVIRONMENT
import com.openlattice.shuttle.ShuttleCli.Companion.FETCHSIZE
import com.openlattice.shuttle.ShuttleCli.Companion.FLIGHT
import com.openlattice.shuttle.ShuttleCli.Companion.FROM_EMAIL
import com.openlattice.shuttle.ShuttleCli.Companion.FROM_EMAIL_PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.HELP
import com.openlattice.shuttle.ShuttleCli.Companion.LOCAL_ORIGIN_EXPECTED_ARGS_COUNT
import com.openlattice.shuttle.ShuttleCli.Companion.NOTIFICATION_EMAILS
import com.openlattice.shuttle.ShuttleCli.Companion.PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.S3
import com.openlattice.shuttle.ShuttleCli.Companion.S3_ORIGIN_EXPECTED_ARGS_COUNT
import com.openlattice.shuttle.ShuttleCli.Companion.SMTP_SERVER
import com.openlattice.shuttle.ShuttleCli.Companion.SMTP_SERVER_PORT
import com.openlattice.shuttle.ShuttleCli.Companion.SQL
import com.openlattice.shuttle.ShuttleCli.Companion.TOKEN
import com.openlattice.shuttle.ShuttleCli.Companion.UPLOAD_SIZE
import com.openlattice.shuttle.ShuttleCli.Companion.USER
import com.openlattice.shuttle.ShuttleCli.Companion.XML
import com.openlattice.shuttle.config.IntegrationConfig
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.payload.SimplePayload
import com.openlattice.shuttle.payload.XmlFilesPayload
import com.openlattice.shuttle.source.LocalFileOrigin
import com.openlattice.shuttle.source.S3BucketOrigin
import org.apache.commons.cli.CommandLine
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.io.File
import java.lang.System.exit
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import java.util.function.Supplier
import java.util.stream.Stream
import kotlin.math.max
import kotlin.streams.asSequence

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(ShuttleCli::class.java)
const val DEFAULT_UPLOAD_SIZE = 100000

fun main(args: Array<String>) {

    val configuration: IntegrationConfig
    val environment: RetrofitFactory.Environment
    val cl = ShuttleCli.parseCommandLine(args)
    val payload: Payload
    val flight: Flight
    val createEntitySets: Boolean
    val contacts: Set<String>
    val rowColsToPrint: List<String>

    if (cl.hasOption(HELP)) {
        ShuttleCli.printHelp()
        return
    }

    if (cl.hasOption(FLIGHT)) {
        flight = ObjectMappers.getYamlMapper().readValue(File(cl.getOptionValue(FLIGHT)), Flight::class.java)
    } else {
        System.err.println("A flight is required in order to run shuttle.")
        ShuttleCli.printHelp()
        return
    }

    //You can have a configuration without any JDBC datasources
    when {
        cl.hasOption(CONFIGURATION) -> {
            configuration = ObjectMappers.getYamlMapper()
                    .readValue(File(cl.getOptionValue(CONFIGURATION)), IntegrationConfig::class.java)

            if (!cl.hasOption(DATASOURCE)) {
                // check datasource presence
                System.out.println("Datasource must be specified when doing a JDBC datasource based integration.")
                ShuttleCli.printHelp()
                return
            }
            if (!cl.hasOption(SQL)) {
                // check SQL presence
                System.out.println("SQL expression must be specified when doing a JDBC datasource based integration.")
                ShuttleCli.printHelp()
                return
            }
            if (cl.hasOption(CSV)) {
                // check csv ABsence
                System.out.println("Cannot specify CSV datasource and JDBC datasource simultaneously.")
                ShuttleCli.printHelp()
                return
            }
            if (cl.hasOption(XML)) {
                // check xml Absence
                System.out.println("Cannot specify XML datasource and JDBC datasource simultaneously.")
                ShuttleCli.printHelp()
                return
            }
            if (cl.hasOption(DATA_ORIGIN)) {
                System.out.println("SQL cannot be specified when performing a data origin integration")
                ShuttleCli.printHelp()
                return
            }

            // get JDBC payload
            val hds = configuration.getHikariDatasource(cl.getOptionValue(DATASOURCE))
            val sql = cl.getOptionValue(SQL)
            rowColsToPrint = configuration.primaryKeyColumns

            payload = if (cl.hasOption(FETCHSIZE)) {
                val fetchSize = cl.getOptionValue(FETCHSIZE).toInt()
                logger.info("Using a fetch size of $fetchSize")
                JdbcPayload(hds, sql, fetchSize)
            } else {
                JdbcPayload(hds, sql)
            }
        }
        cl.hasOption(CSV) -> {// get csv payload
            if (cl.hasOption(DATA_ORIGIN)) {
                System.out.println("CSV cannot be specified when performing a data origin integration")
                ShuttleCli.printHelp()
                return
            }
            rowColsToPrint = listOf()
            payload = SimplePayload(cl.getOptionValue(CSV))
        }
        cl.hasOption(XML) -> {// get xml payload
            rowColsToPrint = listOf()
            if (cl.hasOption(DATA_ORIGIN)) {
                val arguments = cl.getOptionValues(DATA_ORIGIN);
                val dataOrigin = when (arguments[0]) {
                    "S3" -> {
                        if (arguments.size < S3_ORIGIN_EXPECTED_ARGS_COUNT) {
                            println("Not enough arguments provided for S3 data origin, provide AWS region, S3 URL and bucket name")
                            ShuttleCli.printHelp()
                            exit(1)
                            return
                        }
                        S3BucketOrigin(arguments[2], makeAWSS3Client(arguments[1]))
                    }
                    "local" -> {
                        if (arguments.size < LOCAL_ORIGIN_EXPECTED_ARGS_COUNT) {
                            println("Not enough arguments provided for local data origin, provide a local file path")
                            ShuttleCli.printHelp()
                            exit(1)
                            return
                        }
                        LocalFileOrigin(Paths.get(arguments[1]))
                    }
                    else -> {
                        println("The specified configuration is invalid ${cl.getOptionValues(DATA_ORIGIN)}")
                        ShuttleCli.printHelp()
                        exit(1)
                        return
                    }
                }
                payload = XmlFilesPayload(dataOrigin)
            } else {
                payload = XmlFilesPayload(cl.getOptionValue(XML))
            }
        }
        else -> {
            System.err.println("At least one valid data origin must be specified.")
            ShuttleCli.printHelp()
            exit(1)
            return
        }
    }

    environment = if (cl.hasOption(ENVIRONMENT)) {
        val env = cl.getOptionValue(ENVIRONMENT).toUpperCase()
        if ("PRODUCTION" == env) {
            MissionControl.fail(
                    -999, flight, Throwable(
                    "PRODUCTION is not a valid integration environment. The valid environments are PROD_INTEGRATION and LOCAL"
            )
            )
        }
        RetrofitFactory.Environment.valueOf(env)
    } else {
        RetrofitFactory.Environment.PROD_INTEGRATION
    }


    val s3BucketUrl = if (cl.hasOption(S3)) {
        val bucketCategory = cl.getOptionValue(S3)
        check(bucketCategory.toUpperCase() in setOf("TEST", "PRODUCTION")) { "Invalid option $bucketCategory for $S3." }
        when (bucketCategory) {
            "TEST" -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            "PRODUCTION" -> "http://openlattice-media-storage.s3-website-us-gov-west-1.amazonaws.com"
            else -> "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
        }
    } else {
        "https://tempy-media-storage.s3-website-us-gov-west-1.amazonaws.com"
    }

    //TODO: Use the right method to select the JWT token for the appropriate environment.

    val missionControl = when {
        cl.hasOption(TOKEN) -> {
            Preconditions.checkArgument(!cl.hasOption(PASSWORD))
            val jwt = cl.getOptionValue(TOKEN)
            MissionControl(environment, Supplier { jwt }, s3BucketUrl)
        }
        cl.hasOption(USER) -> {
            Preconditions.checkArgument(cl.hasOption(PASSWORD))
            val user = cl.getOptionValue(USER)
            val password = cl.getOptionValue(PASSWORD)
            MissionControl(environment, user, password, s3BucketUrl)
        }
        else -> {
            System.err.println("User or token must be provided for authentication.")
            ShuttleCli.printHelp()
            return
        }
    }

    createEntitySets = cl.hasOption(CREATE)
    if (createEntitySets) {
        if (environment == RetrofitFactory.Environment.PRODUCTION) {
            throw IllegalArgumentException(
                    "It is not allowed to automatically create entity sets on " +
                            "${RetrofitFactory.Environment.PRODUCTION} environment"
            )
        }

        contacts = cl.getOptionValues(CREATE).toSet()
        if (contacts.isEmpty()) {
            System.err.println("Can't create entity sets automatically without contacts provided")
            ShuttleCli.printHelp()
            return
        }
    } else {
        contacts = setOf()
    }

    val uploadBatchSize = if (cl.hasOption(UPLOAD_SIZE)) {
        cl.getOptionValue(UPLOAD_SIZE).toInt()
    } else {
        DEFAULT_UPLOAD_SIZE
    }

    val emailConfiguration = getEmailConfiguration(cl)

    val flightPlan = mapOf(flight to payload)

    try {
        MissionControl.setEmailConfiguration(emailConfiguration)
        val shuttle = missionControl.prepare(flightPlan, createEntitySets, rowColsToPrint, contacts)
        shuttle.launch(uploadBatchSize)
        MissionControl.succeed()
    } catch (ex: Exception) {
        MissionControl.fail(1, flight, ex)
    }
}

fun makeAWSS3Client(region: String): AmazonS3 {
    return AmazonS3ClientBuilder
            .standard()
            .withPathStyleAccessEnabled(true)
            .withRegion(region)
            .build();
}

fun getEmailConfiguration(cl: CommandLine): Optional<EmailConfiguration> {
    return when {
        cl.hasOption(SMTP_SERVER) -> {
            val smtpServer = cl.getOptionValue(SMTP_SERVER)
            val smtpServerPort = if (cl.hasOption(SMTP_SERVER_PORT)) {
                cl.getOptionValue(SMTP_SERVER_PORT).toInt()
            } else {
                System.err.println("No smtp server port was specified")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val notificationEmails = cl.getOptionValues(NOTIFICATION_EMAILS).toSet()
            if (notificationEmails.isEmpty()) {
                System.err.println("No notification e-mails were actually specified.")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val fromEmail = if (cl.hasOption(FROM_EMAIL)) {
                cl.getOptionValue(FROM_EMAIL)
            } else {
                System.err.println("If notification e-mails are specified must also specify a sending account.")
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }

            val fromEmailPassword = if (cl.hasOption(FROM_EMAIL_PASSWORD)) {
                cl.getOptionValue(FROM_EMAIL_PASSWORD)
            } else {
                System.err.println(
                        "If notification e-mails are specified must also specify an e-mail password for sending account."
                )
                ShuttleCli.printHelp()
                kotlin.system.exitProcess(1)
            }
            Optional.of(
                    EmailConfiguration(fromEmail, fromEmailPassword, notificationEmails, smtpServer, smtpServerPort)
            )
        }
        cl.hasOption(SMTP_SERVER_PORT) -> {
            System.err.println("Port was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL) -> {
            System.err.println("From e-mail was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(FROM_EMAIL_PASSWORD) -> {
            System.err.println("From e-mail password was specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        cl.hasOption(NOTIFICATION_EMAILS) -> {
            System.err.println("Notification e-mails were specified, no smtp server was specified")
            ShuttleCli.printHelp()
            kotlin.system.exitProcess(1)
        }
        else -> Optional.empty()
    }

}


/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */
class Shuttle(
        private val flightPlan: Map<Flight, Payload>,
        private val entitySets: Map<String, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<FullQualifiedName, PropertyType>,
        private val propertyTypesById: Map<UUID, PropertyType>,
        private val integrationDestinations: Map<StorageDestination, IntegrationDestination>,
        private val dataIntegrationApi: DataIntegrationApi,
        private val tableColsToPrint: List<String>
) {
    private val uploadingExecutor = Executors.newSingleThreadExecutor()

    companion object {
        private val logger = LoggerFactory.getLogger(Shuttle::class.java)
        private val metrics = MetricRegistry()
        private val uploadRate = metrics.meter(MetricRegistry.name(Shuttle::class.java, "uploads"))
        private val reporter = Slf4jReporter.forRegistry(metrics)
                .outputTo(LoggerFactory.getLogger(Shuttle::class.java))
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build()

        init {

            reporter.start(1, TimeUnit.MINUTES)

        }
    }

    private val updateTypes = flightPlan.keys.flatMap { flight ->
        flight.entities.map { entitySets[it.entitySetName]!!.id to it.updateType } +
                flight.associations.map { entitySets[it.entitySetName]!!.id to it.updateType }
    }.toMap()


    fun launch(uploadBatchSize: Int): Long {
        val sw = Stopwatch.createStarted()
        val total = flightPlan.entries.map { entry ->
            logger.info("Launching flight: {}", entry.key.name)
            val count = takeoff(entry.key, entry.value.payload, uploadBatchSize, tableColsToPrint)
            logger.info("Finished flight: {}", entry.key.name)
            count
        }.sum()
        logger.info("Executed {} entity writes in flight plan in {} ms.", total, sw.elapsed(TimeUnit.MILLISECONDS))
        return total
    }

    /**
     * This function works under the assumption that the set returned from key is a unmodifiable linked hash set.
     */
    private fun getKeys(entitySetName: String): Set<UUID> {
        return entityTypes[entitySets[entitySetName]!!.entityTypeId]!!.key
    }

    /**
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     *
     * @param key A stable set of ordered primary key property type ids to use for default entity key generation.
     */
    private fun generateDefaultEntityId(
            key: Set<UUID>,
            properties: Map<UUID, Set<Any>>
    ): String {
        val keyValuesPresent = key.filter { !properties[it].isNullOrEmpty() }.isNotEmpty()

        return if (keyValuesPresent) ApiUtil.generateDefaultEntityId(key.stream(), properties) else ""
    }

    private fun takeoff(
            flight: Flight, payload: Stream<Map<String, Any>>, uploadBatchSize: Int, rowColsToPrint: List<String>
    ): Long {
        val integratedEntities = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integratedEdges = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integrationQueue = Queues
                .newArrayBlockingQueue<List<Map<String, Any>>>(
                        max(2, 2 * (Runtime.getRuntime().availableProcessors() - 2))
                )
        val rows = LongAdder()
        val sw = Stopwatch.createStarted()
        val remaining = AtomicLong(0)
        val batchCounter = AtomicLong(0)
        val minRows = ConcurrentSkipListMap<Long, Map<String, Any>>()

        uploadingExecutor.execute {
            Stream.generate { integrationQueue.take() }.parallel()
                    .map { batch ->
                        try {
                            rows.add(batch.size.toLong())
                            val batchCtr = batchCounter.incrementAndGet()
                            minRows[batchCtr] = batch[0]
                            return@map impulse(flight, batch, batchCtr)
                        } catch (ex: Exception) {
                            MissionControl.fail(1, flight, ex, listOf(uploadingExecutor))
                        } catch (err: OutOfMemoryError) {
                            MissionControl.fail(1, flight, err, listOf(uploadingExecutor))
                        }
                    }
                    .forEach { batch ->
                        try {
                            val sw = Stopwatch.createStarted()
                            val entityKeys = (batch.entities.flatMap { e -> e.value.map { it.key } }
                                    + batch.associations.flatMap { it.value.map { assoc -> assoc.key } }).toSet()
                            val entityKeyIds = entityKeys.zip(
                                    dataIntegrationApi.getEntityKeyIds(entityKeys)
                            ).toMap()

                            logger.info(
                                    "Generated {} entity key ids in {} ms",
                                    entityKeys.size,
                                    sw.elapsed(TimeUnit.MILLISECONDS)
                            )

                            integrationDestinations.forEach { (storageDestination, integrationDestination) ->
                                if (batch.entities.containsKey(storageDestination)) {
                                    integratedEntities.getOrPut(storageDestination) { AtomicLong(0) }.addAndGet(
                                            integrationDestination.integrateEntities(
                                                    batch.entities.getValue(storageDestination),
                                                    entityKeyIds,
                                                    updateTypes
                                            )
                                    )
                                }

                                if (batch.associations.containsKey(storageDestination)) {
                                    integratedEdges.getOrPut(storageDestination) { AtomicLong(0) }.addAndGet(
                                            integrationDestination.integrateAssociations(
                                                    batch.associations.getValue(storageDestination),
                                                    entityKeyIds,
                                                    updateTypes
                                            )
                                    )
                                }
                            }

                            minRows.remove(batch.batchId)
                            uploadRate.mark(entityKeys.size.toLong())
                            logger.info("Processed {} rows.", rows.sum())
                            logger.info("Current entities progress: {}", integratedEntities)
                            logger.info("Current edges progress: {}", integratedEdges)
                        } catch (ex: Exception) {
                            if (rowColsToPrint.isNotEmpty()) {
                                logger.info("Earliest unintegrated row:")
                                printRow(minRows.firstEntry().value, rowColsToPrint)
                            }
                            MissionControl.fail(1, flight, ex, listOf(uploadingExecutor))
                        } catch (err: OutOfMemoryError) {
                            if (rowColsToPrint.isNotEmpty()) {
                                logger.info("Earliest unintegrated row:")
                                printRow(minRows.firstEntry().value, rowColsToPrint)
                            }
                            MissionControl.fail(1, flight, err, listOf(uploadingExecutor))
                        } finally {
                            remaining.decrementAndGet()
                        }
                    }
        }

        payload.asSequence()
                .chunked(uploadBatchSize)
                .forEach {
                    remaining.incrementAndGet()
                    integrationQueue.put(it)
                }
        //Wait on upload thread to finish emptying queue.
        try {
            while (remaining.get() > 0) {
                logger.info("Waiting on upload to finish... ${remaining.get()} batches left")
                Thread.sleep(1000)
            }
        } catch (ex: Exception) {
            MissionControl.fail(1, flight, ex, listOf(uploadingExecutor))
        }

        return StorageDestination.values().map {
            logger.info(
                    "Integrated {} entities and {} edges in {} ms for flight {} to {}",
                    integratedEntities.getValue(it),
                    integratedEdges.getValue(it),
                    sw.elapsed(TimeUnit.MILLISECONDS),
                    flight.name,
                    it.name
            )
            integratedEntities.getValue(it).get() + integratedEdges.getValue(it).get()
        }.sum()
    }

    private fun printRow(row: Map<String, Any>, rowColsToPrint: List<String>) {
        var rowHeaders = ""
        var contents = ""
        rowColsToPrint.forEach { colName ->
            rowHeaders += "$colName\t|\t"
            contents += "${row[colName].toString()}\t|\t"
        }
        logger.info("Row Contents:\n$rowHeaders\n$contents")
    }

    private fun buildPropertiesFromPropertyDefinitions(
            row: Map<String, Any>,
            propertyDefinitions: Collection<PropertyDefinition>
    )
            : Pair<MutableMap<UUID, MutableSet<Any>>, MutableMap<StorageDestination, MutableMap<UUID, MutableSet<Any>>>> {
        val properties = mutableMapOf<UUID, MutableSet<Any>>()
        val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()

        for (propertyDefinition in propertyDefinitions) {
            val propertyValue = propertyDefinition.propertyValue.apply(row)

            if (propertyValue == null || !((propertyValue !is String) || propertyValue.isNotBlank())) {
                continue
            }

            val propertyType = propertyTypes.getValue(propertyDefinition.fullQualifiedName)

            val storageDestination = propertyDefinition.storageDestination.orElseGet {
                when (propertyType.datatype) {
                    EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                    else -> StorageDestination.REST
                }
            }

            val propertyValueAsCollection: Collection<Any> =
                    if (propertyValue is Collection<*>) propertyValue as Collection<Any>
                    else ImmutableList.of(propertyValue)

            val propertyId = propertyType.id

            addressedProperties
                    .getOrPut(storageDestination) { mutableMapOf() }
                    .getOrPut(propertyId) { mutableSetOf() }
                    .addAll(propertyValueAsCollection)
            properties.getOrPut(propertyId) { mutableSetOf() }.addAll(propertyValueAsCollection)
        }
        return Pair(properties, addressedProperties);
    }

    private fun impulse(flight: Flight, batch: List<Map<String, Any>>, batchNumber: Long): AddressedDataHolder {
        val addressedDataHolder = AddressedDataHolder(mutableMapOf(), mutableMapOf(), batchNumber)

        batch.forEach { row ->
            val aliasesToEntityKey = mutableMapOf<String, EntityKey>()
            val wasCreated = mutableMapOf<String, Boolean>()
            if (flight.condition.isPresent && !(flight.valueMapper.apply(row) as Boolean)) {
                return@forEach
            }
            for (entityDefinition in flight.entities) {
                val condition = if (entityDefinition.condition.isPresent) {
                    entityDefinition.valueMapper.apply(row) as Boolean
                } else {
                    true
                }

                val (properties, addressedProperties) = buildPropertiesFromPropertyDefinitions(
                        row, entityDefinition.properties
                )

                /*
                 * For entityId generation to work correctly it is very important that Stream remain ordered.
                 * Ordered != sequential vs parallel.
                 */

                val entityId = entityDefinition.generator
                        .map { it.apply(row) }
                        .orElseGet {
                            generateDefaultEntityId(getKeys(entityDefinition.entitySetName), properties)
                        }

                if (StringUtils.isNotBlank(entityId) and condition and properties.isNotEmpty()) {

                    val entitySetId = entitySets[entityDefinition.entitySetName]!!.id

                    val key = EntityKey(entitySetId, entityId)
                    aliasesToEntityKey[entityDefinition.alias] = key
                    addressedProperties.forEach { storageDestination, data ->
                        addressedDataHolder.entities
                                .getOrPut(storageDestination) { mutableSetOf() }
                                .add(Entity(key, data))
                    }
                    wasCreated[entityDefinition.alias] = true
                } else {
                    wasCreated[entityDefinition.alias] = false
                }
            }

            for (associationDefinition in flight.associations) {

                if (associationDefinition.condition.isPresent &&
                        !(associationDefinition.valueMapper.apply(row) as Boolean)) {
                    continue
                }

                if (!wasCreated.containsKey(associationDefinition.dstAlias)) {
                    logger.error(
                            "Destination " + associationDefinition.dstAlias
                                    + " cannot be found to construct association " + associationDefinition.alias
                    )
                }

                if (!wasCreated.containsKey(associationDefinition.srcAlias)) {
                    logger.error(
                            ("Source " + associationDefinition.srcAlias
                                    + " cannot be found to construct association " + associationDefinition.alias)
                    )
                }
                if ((wasCreated[associationDefinition.srcAlias]!! && wasCreated[associationDefinition.dstAlias]!!)) {

                    val (properties, addressedProperties) = buildPropertiesFromPropertyDefinitions(
                            row, associationDefinition.properties
                    )

                    val entityId = associationDefinition.generator
                            .map { it.apply(row) }
                            .orElseGet {
                                generateDefaultEntityId(getKeys(associationDefinition.entitySetName), properties)
                            }

                    if (StringUtils.isNotBlank(entityId)) {

                        val entitySetId = entitySets.getValue(associationDefinition.entitySetName).id

                        val key = EntityKey(entitySetId, entityId)
                        val src = aliasesToEntityKey[associationDefinition.srcAlias]
                        val dst = aliasesToEntityKey[associationDefinition.dstAlias]
                        addressedProperties.forEach { storageDestination, data ->
                            addressedDataHolder.associations
                                    .getOrPut(storageDestination) { mutableSetOf() }
                                    .add(Association(key, src, dst, data))

                        }
                    } else {
                        logger.error(
                                "Encountered blank entity id for entity set {}",
                                associationDefinition.entitySetName
                        )
                    }
                }
            }
        }
        return addressedDataHolder
    }
}
