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

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.Slf4jReporter
import com.google.common.base.Stopwatch
import com.google.common.collect.ImmutableList
import com.google.common.collect.Queues
import com.google.common.util.concurrent.ListeningExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.openlattice.ApiUtil
import com.openlattice.data.DataIntegrationApi
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.*
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.payload.Payload

import com.openlattice.shuttle.payload.CsvPayload
import com.openlattice.shuttle.payload.XmlFilesPayload
import com.openlattice.shuttle.source.LocalFileOrigin
import com.openlattice.shuttle.source.S3BucketOrigin
import org.apache.commons.cli.CommandLine

import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.LongAdder
import java.util.stream.Stream
import kotlin.math.max
import kotlin.streams.asSequence
import kotlin.streams.asStream

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */


private val logger = LoggerFactory.getLogger(Shuttle::class.java)
const val DEFAULT_UPLOAD_SIZE = 100_000



/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */
class Shuttle(
        private val flightPlan: Map<Flight, Payload>,
        private val entitySets: Map<String, EntitySet>,
        private val entityTypes: Map<UUID, EntityType>,
        private val propertyTypes: Map<FullQualifiedName, PropertyType>,
        private val integrationDestinations: Map<StorageDestination, IntegrationDestination>,
        private val dataIntegrationApi: DataIntegrationApi,
        private val tableColsToPrint: List<String>,
        private val parameters: MissionParameters,
        private val uploadingExecutor: ListeningExecutorService = MoreExecutors.listeningDecorator(
                Executors.newSingleThreadExecutor()
        )
) {
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
            reporter.start(15, TimeUnit.SECONDS)
        }
    }


    private fun takeoff(
            flight: Flight,
            payload: Stream<Map<String, Any>>,
            uploadBatchSize: Int,
            rowColsToPrint: List<String>
    ): Long {
        val integratedEntities = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integratedEdges = mutableMapOf<StorageDestination, AtomicLong>().withDefault { AtomicLong(0L) }
        val integrationQueue = Queues
                .newArrayBlockingQueue<List<Map<String, Any>>>(
                        max(2, 2 * (Runtime.getRuntime().availableProcessors() - 2))
                )
        val integrationSequence = generateSequence { integrationQueue.take() }
        val rows = LongAdder()
        val sw = Stopwatch.createStarted()
        val remaining = AtomicLong(0)
        val batchCounter = AtomicLong(0)
        val minRows = ConcurrentSkipListMap<Long, Map<String, Any>>()
        val integrationCompletedLatch = CountDownLatch(1);

        uploadingExecutor.execute {
            integrationSequence.asStream().parallel().map { batch ->
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
            }.forEach { batch ->
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
                    if( remaining.decrementAndGet() == 0L ){
                        integrationCompletedLatch.countDown()
                    } else {
                        logger.info("There are ${remaining.get()} batches remaining for upload." )
                    }
                }
            }
        }

        payload.asSequence()
                .chunked(uploadBatchSize)
                .forEach {
                    remaining.incrementAndGet()
                    integrationQueue.put(it)
                    logger.info("There are ${remaining.get()} batches remaining for upload." )
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
                    else -> if (parameters.postgres.enabled) StorageDestination.POSTGRES else StorageDestination.REST
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
                    addressedProperties.forEach { (storageDestination, data) ->
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
                        addressedProperties.forEach { (storageDestination, data) ->
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

    private val updateTypes = flightPlan.keys.flatMap { flight ->
        flight.entities.map { entitySets.getValue(it.entitySetName).id to it.updateType } +
                flight.associations.map { entitySets.getValue(it.entitySetName).id to it.updateType }
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
        return entityTypes.getValue(entitySets.getValue(entitySetName).entityTypeId).key
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
        val keyValuesPresent = key.any { !properties[it].isNullOrEmpty() }

        return if (keyValuesPresent) ApiUtil.generateDefaultEntityId(key.stream(), properties) else ""
    }
}
