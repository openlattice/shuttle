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

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions
import com.google.common.base.Stopwatch
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
import com.openlattice.shuttle.ShuttleCli.Companion.ENVIRONMENT
import com.openlattice.shuttle.ShuttleCli.Companion.FLIGHT
import com.openlattice.shuttle.ShuttleCli.Companion.HELP
import com.openlattice.shuttle.ShuttleCli.Companion.PASSWORD
import com.openlattice.shuttle.ShuttleCli.Companion.S3
import com.openlattice.shuttle.ShuttleCli.Companion.SQL
import com.openlattice.shuttle.ShuttleCli.Companion.TOKEN
import com.openlattice.shuttle.ShuttleCli.Companion.USER
import com.openlattice.shuttle.config.IntegrationConfig
import com.openlattice.shuttle.payload.JdbcPayload
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.payload.SimplePayload
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.io.File
import java.lang.System.exit
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Supplier
import java.util.stream.Stream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */

private val logger = LoggerFactory.getLogger(ShuttleCli::class.java)

fun main(args: Array<String>) {

    val configuration: IntegrationConfig
    val environment: RetrofitFactory.Environment
    val cl = ShuttleCli.parseCommandLine(args)
    val payload: Payload
    val flight: Flight
    val createEntitySets: Boolean
    val contacts: Set<String>


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

    //You can have a configuration without any JDBC datasrouces
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

            // get JDBC payload
            val hds = configuration.getHikariDatasource(cl.getOptionValue(DATASOURCE))
            val sql = cl.getOptionValue(SQL)
            payload = JdbcPayload(hds, sql)

        }
        cl.hasOption(CSV) -> // get csv payload
            payload = SimplePayload(cl.getOptionValue(CSV))
        else -> {
            System.err.println("At least one valid data source must be specified.")
            ShuttleCli.printHelp()
            exit(1)
            return
        }
    }


    environment = if (cl.hasOption(ENVIRONMENT)) {
        RetrofitFactory.Environment.valueOf(cl.getOptionValue(ENVIRONMENT).toUpperCase())
    } else {
        RetrofitFactory.Environment.PRODUCTION
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
            throw IllegalArgumentException("Can't create entity sets automatically without contacts provided")
        }
    } else {
        contacts = setOf()
    }


    val flightPlan = mapOf(flight to payload)

    val shuttle = missionControl.prepare(flightPlan, createEntitySets, contacts)
    shuttle.launch()
}


const val UPLOAD_BATCH_SIZE = 100000

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
        private val dataIntegrationApi: DataIntegrationApi
) {
    companion object {
        private val logger = LoggerFactory.getLogger(Shuttle::class.java)
    }

    private val updateTypes = flightPlan.keys.flatMap { flight ->
        flight.entities.map { entitySets[it.entitySetName]!!.id to it.updateType } +
                flight.associations.map { entitySets[it.entitySetName]!!.id to it.updateType }
    }.toMap()


    fun launch(): Long {
        val sw = Stopwatch.createStarted()
        val total = flightPlan.entries.map { entry ->
            logger.info("Launching flight: {}", entry.key.name)
            val count = takeoff(entry.key, entry.value.payload)
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
        return ApiUtil.generateDefaultEntityId(key.stream(), properties)
    }

    private fun takeoff(flight: Flight, payload: Stream<Map<String, Any>>): Long {
        val sw = Stopwatch.createStarted()
        val remaining = payload.parallel().map { row ->

            val aliasesToEntityKey = mutableMapOf<String, EntityKey>()
            val addressedDataHolder = AddressedDataHolder(mutableMapOf(), mutableMapOf())
//            val entities = mutableSetOf<Entity>()
//            val associations = mutableSetOf<Association>()
            val wasCreated = mutableMapOf<String, Boolean>()

            if (flight.condition.isPresent && !(flight.valueMapper.apply(row) as Boolean)) {
                return@map addressedDataHolder
                //BulkDataCreation(entities, associations)
            }

            for (entityDefinition in flight.entities) {
                val condition = if (entityDefinition.condition.isPresent) {
                    entityDefinition.valueMapper.apply(row) as Boolean
                } else {
                    true
                }

                val entitySetId = entitySets[entityDefinition.entitySetName]!!.id
                val properties = mutableMapOf<UUID, MutableSet<Any>>()
                val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()
                for (propertyDefinition in entityDefinition.properties) {
                    val propertyValue = propertyDefinition.propertyValue.apply(row)
                    if (propertyValue != null &&
                            ((propertyValue !is String) || propertyValue.isNotBlank())) {
                        val storageDestination = propertyDefinition.storageDestination.orElseGet {
                            when (propertyTypes[propertyDefinition.fullQualifiedName]!!.datatype) {
                                EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                else -> StorageDestination.REST
                            }
                        }

                        val propertyId = propertyTypes[propertyDefinition.fullQualifiedName]!!.id

                        val propertyValueAsCollection: Collection<Any> =
                                if (propertyValue is Collection<*>) propertyValue as Collection<Any>
                                else mutableSetOf(propertyValue)

                        addressedProperties
                                .getOrPut(storageDestination) { mutableMapOf() }
                                .getOrPut(propertyId) { mutableSetOf() }
                                .addAll(propertyValueAsCollection)
                        properties.getOrPut(propertyId) { mutableSetOf() }.addAll(propertyValueAsCollection)
                    }
                }

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
                    val key = EntityKey(entitySetId, entityId)
                    aliasesToEntityKey[entityDefinition.alias] = key
                    addressedProperties.forEach { storageDestination, data ->
                        addressedDataHolder.entities
                                .getOrPut(storageDestination) { mutableSetOf() }
                                .add(Entity(key, data))
                    }
//                    entities.add(Entity(key, properties))
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

                    val entitySetId = entitySets[associationDefinition.entitySetName]!!.id
                    val properties = mutableMapOf<UUID, MutableSet<Any>>()
                    val addressedProperties = mutableMapOf<StorageDestination, MutableMap<UUID, MutableSet<Any>>>()

                    for (propertyDefinition in associationDefinition.properties) {
                        val propertyValue = propertyDefinition.propertyValue.apply(row)
                        if (propertyValue != null &&
                                ((propertyValue !is String) || propertyValue.isNotBlank())) {

                            val storageDestination = propertyDefinition.storageDestination.orElseGet {
                                when (propertyTypes[propertyDefinition.fullQualifiedName]!!.datatype) {
                                    EdmPrimitiveTypeKind.Binary -> StorageDestination.S3
                                    else -> StorageDestination.REST
                                }
                            }

                            val propertyId = propertyTypes[propertyDefinition.fullQualifiedName]!!.id

                            val propertyValueAsCollection: Collection<Any> =
                                    if (propertyValue is Collection<*>) propertyValue as Collection<Any>
                                    else mutableSetOf(propertyValue)

                            addressedProperties
                                    .getOrPut(storageDestination) { mutableMapOf() }
                                    .getOrPut(propertyId) { mutableSetOf() }
                                    .addAll(propertyValueAsCollection)
                            properties.getOrPut(propertyId) { mutableSetOf() }.addAll(propertyValueAsCollection)
                        }
                    }

                    val entityId = associationDefinition.generator
                            .map { it.apply(row) }
                            .orElseGet {
                                ApiUtil.generateDefaultEntityId(
                                        getKeys(associationDefinition.entitySetName).stream(),
                                        properties
                                )
                            }

                    if (StringUtils.isNotBlank(entityId)) {
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
            addressedDataHolder
        }.reduce { a: AddressedDataHolder, b: AddressedDataHolder ->
            b.associations.forEach { storageDestination, associations ->
                a.associations.getOrPut(storageDestination) { mutableSetOf() }.addAll(associations)
            }

            b.entities.forEach { storageDestination, entities ->
                a.entities.getOrPut(storageDestination) { mutableSetOf() }.addAll(entities)
            }

            a.integratedEntities.forEach { sd, count -> count.addAndGet(b.integratedEntities[sd]!!.get()) }
            a.integratedEdges.forEach { sd, count -> count.addAndGet(b.integratedEdges[sd]!!.get()) }

            if (a.associations.values.any { it.size > UPLOAD_BATCH_SIZE } ||
                    a.entities.values.any { it.size > UPLOAD_BATCH_SIZE }) {
                val entityKeys = (a.entities.flatMap { e -> e.value.map { it.key } }
                        + a.associations.flatMap { it.value.map { it.key } }).toSet()
                val entityKeyIds = entityKeys.zip(dataIntegrationApi.getEntityKeyIds(entityKeys)).toMap()
                val adh = AddressedDataHolder(mutableMapOf(), mutableMapOf())

                integrationDestinations.forEach {
                    if (a.entities.containsKey(it.key)) {
                        adh.integratedEntities[it.key]!!
                                .addAndGet(it.value.integrateEntities(a.entities[it.key]!!, entityKeyIds, updateTypes))
                    }
                    if (a.associations.containsKey(it.key)) {
                        adh.integratedEdges[it.key]!!
                                .addAndGet(
                                        it.value.integrateAssociations(
                                                a.associations[it.key]!!,
                                                entityKeyIds,
                                                updateTypes
                                        )
                                )
                    }
                }

                logger.info("Current entities progress: {}", adh.integratedEntities)
                logger.info("Current edges progress: {}", adh.integratedEdges)

                return@reduce adh
            }
            a
        }

        val (integratedEntities, integratedEdges) = remaining.map { r ->
            val entityKeys = (r.entities.flatMap { it.value.map { it.key } } + r.associations.flatMap { it.value.map { it.key } }).toSet()
            val entityKeyIds = entityKeys.zip(dataIntegrationApi.getEntityKeyIds(entityKeys)).toMap()
            integrationDestinations
                    .forEach {
                        if (r.entities.containsKey(it.key)) {
                            r.integratedEntities[it.key]!!
                                    .addAndGet(
                                            it.value.integrateEntities(r.entities[it.key]!!, entityKeyIds, updateTypes)
                                    )
                        }
                        if (r.associations.containsKey(it.key)) {
                            r.integratedEdges[it.key]!!
                                    .addAndGet(
                                            it.value.integrateAssociations(
                                                    r.associations[it.key]!!, entityKeyIds, updateTypes
                                            )
                                    )
                        }
                    }
            r.integratedEntities to r.integratedEdges
        }.orElse(
                StorageDestination.values().map { it to AtomicLong(0) }.toMap() to
                        StorageDestination.values().map { it to AtomicLong(0) }.toMap()
        )

        return StorageDestination.values().map {
            logger.info(
                    "Integrated {} entities and {} edges in {} ms for flight {} to {}",
                    integratedEntities,
                    integratedEdges,
                    sw.elapsed(TimeUnit.MILLISECONDS),
                    flight.name,
                    it.name
            )
            integratedEntities[it]!!.get() + integratedEdges[it]!!.get()
        }.sum()
    }

}