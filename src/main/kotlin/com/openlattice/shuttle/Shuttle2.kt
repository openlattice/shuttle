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
import com.google.common.cache.CacheBuilder
import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.collect.ImmutableMap
import com.google.common.collect.Sets
import com.openlattice.ApiUtil
import com.openlattice.ApiUtil.generateDefaultEntityId
import com.openlattice.client.ApiClient
import com.openlattice.client.ApiFactoryFactory
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.EntityKey
import com.openlattice.data.integration.Association
import com.openlattice.data.integration.BulkDataCreation
import com.openlattice.data.integration.Entity
import com.openlattice.data.integration.StorageDestination
import com.openlattice.data.serializers.FullQualifiedNameJacksonSerializer
import com.openlattice.edm.EdmApi
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.AssociationType
import com.openlattice.edm.type.PropertyType
import com.openlattice.shuttle.payload.Payload
import com.openlattice.shuttle.serialization.JacksonLambdaDeserializer
import com.openlattice.shuttle.serialization.JacksonLambdaSerializer
import org.apache.commons.lang3.StringUtils
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind
import org.apache.olingo.commons.api.edm.FullQualifiedName
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.ExecutionException
import java.util.function.Supplier
import java.util.stream.Collectors
import java.util.stream.Stream

/**
 *
 * This is the primary class for driving an integration. It is designed to cache all
 */

class Shuttle2(
        flightPlan: Map<Flight, Payload>,
        authToken: String,
        environment: RetrofitFactory.Environment = RetrofitFactory.Environment.LOCAL
) {
    private val apiClient = ApiClient(environment) { authToken }
    private val edmApi = apiClient.edmApi
    private val dataApi = apiClient.dataApi
    private val dataIntegrationApi = apiClient.dataIntegrationApi
    private val s3Api = apiClient.s3Api

    private val storageDestByProperty = ConcurrentHashMap<UUID, ConcurrentMap<UUID, StorageDestination>>()
    private val seenEntitySetIds = Sets.newHashSet<UUID>()

    private val logger = LoggerFactory
            .getLogger(Shuttle::class.java)

    private val UPLOAD_BATCH_SIZE = 100000

    private val entitySetIdCache = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .build(object : CacheLoader<String, UUID>() {
                @Throws(Exception::class)
                override fun load(entitySetName: String): UUID {
                    return edmApi.getEntitySetId(entitySetName)
                }
            })

    private val entitySets = getEntitySets(flightPlan.keys)
    private val associationSets = getAssociations(flightPlan.keys)

    val propertiesCache = edmApi.propertyTypes.map { it.type to it }.toMap()
    private val propertyIdsCache = propertiesCache.mapValues { it.value.id }

    private val keyCache = edmApi.getEntityS
    private val keyCache = CacheBuilder
            .newBuilder()
            .maximumSize(1000)
            .build<String, LinkedHashSet<FullQualifiedName>>(
                    object : CacheLoader<String, LinkedHashSet<FullQualifiedName>>() {
                        @Throws(Exception::class)
                        override fun load(entitySetName: String): LinkedHashSet<FullQualifiedName> {
                            return edmApi.getEntityType(
                                    edmApi.getEntitySet(edmApi.getEntitySetId(entitySetName)).entityTypeId
                            )
                                    .key
                                    .map { propertyTypeId -> edmApi.getPropertyType(propertyTypeId).getType() }
                                    .toCollection(LinkedHashSet())
                        }
                    })

    private fun getPropertyTypes( flights: Collection<Flight>) : Map<FullQualifiedName, PropertyType> {
        flights.flatMap { it.entityDefinitions + it.associationDefinitions.values.map  }
    }
    private fun getAssociations(flights: Collection<Flight>): Map<String, EntitySet> {
        return flights.flatMap {
            it.associationDefinitions.map {
                it.value.entitySetName to edmApi.getEntitySet(
                        edmApi.getEntitySetId(it.value.entitySetName)
                )
            }
        }.toMap()
    }

    private fun getEntitySets(flights: Collection<Flight>): Map<String, EntitySet> {
        //TODO: Make this more efficient if startup is slow.
        return flights.flatMap {
            it.entityDefinitions.map {
                it.value.entitySetName to edmApi.getEntitySet(
                        edmApi.getEntitySetId(it.value.entitySetName)
                )
            }
        }.toMap()
    }

    @Throws(InterruptedException::class)
    fun launchPayloadFlight(flightsToPayloads: Map<Flight, Payload>) {
        ensureValidIntegration(flightsToPayloads)
        logger.info("Launching flight: {}", entry.key.name)
        logger.info("Finished flight: {}", entry.key.name)
        flightsToPayloads.entries.forEach { entry ->

            launchFlight(entry.key, entry.value.payload)

        }

        System.exit(0)

    }

    /*
     * By default, the entity id is generated as a concatenation of the entity set id and all the key property values.
     * This is guaranteed to be unique for each unique set of primary key values. For this to work correctly it is very
     * important that Stream remain ordered. Ordered != sequential vs parallel.
     */
    private fun generateDefaultEntityId(
            key: LinkedHashSet<FullQualifiedName>,
            properties: Map<UUID, Set<Any>>
    ): String {
        return ApiUtil.generateDefaultEntityId(key.stream().map { propertyIdsCache[it]!! }, properties)
    }

    fun launchFlight(flight: Flight, payload: Stream<Map<String, Any>>) {
        logger.info("Launching flight: {}", flight.name)
        val remaining = payload.parallel().map { row ->

            val aliasesToEntityKey = mutableMapOf<String, EntityKey>()
            val entities = mutableSetOf<Entity>()
            val associations = mutableSetOf<Association>()
            val wasCreated = mutableMapOf<String, Boolean>()

            if (flight.condition.isPresent) {
                val out = flight.valueMapper.apply(row)
                if (!(out as Boolean)) {
                    return@map BulkDataCreation(entities, associations)
                }
            }

            for (entityDefinition in flight.entities) {

                val condition = entityDefinition.condition.map { entityDefinition.valueMapper.apply(row) }.orElse(true)
                val entitySetId = entitySetIdCache.getUnchecked(entityDefinition.entitySetName)
                val properties = HashMap<UUID, MutableSet<Any>>()

                for (propertyDefinition in entityDefinition.properties) {
                    val storageDestination = propertyDefinition.storageDestination.orElseGet { StorageDestination.REST }

                    val propertyValue = propertyDefinition.propertyValue.apply(row)
                    if (propertyValue != null) {
                        val stringProp = propertyValue!!.toString()
                        if (!StringUtils.isBlank(stringProp)) {
                            val propertyId = propertyIdsCache[propertyDefinition.fullQualifiedName]!!
                            properties.getOrPut(propertyId) { mutableSetOf() }.add(propertyValue)
                        }
                    }
                }

                /*
                 * For entityId generation to work correctly it is very important that Stream remain ordered. Ordered !=
                 * sequential vs parallel.
                 */

                val entityId = entityDefinition.generator.map { it.apply(row) }
                        .orElseGet {
                            generateDefaultEntityId(
                                    keyCache.getUnchecked(entityDefinition.entitySetName), properties
                            )
                        }

                if (StringUtils.isNotBlank(entityId) and condition!! and (properties.size > 0)) {
                    val key = EntityKey(entitySetId, entityId)
                    aliasesToEntityKey[entityDefinition.alias] = key
                    entities.add(Entity(key, properties))
                    wasCreated[entityDefinition.alias] = true
                } else {
                    wasCreated[entityDefinition.alias] = false
                }

                MissionControl.signal()
            }

            for (associationDefinition in flight.associations) {

                if (associationDefinition.condition.isPresent) {
                    val out = associationDefinition.valueMapper.apply(row)
                    if (!(out as Boolean)) {
                        continue
                    }
                }

                if (!wasCreated.containsKey(associationDefinition.dstAlias)) {
                    com.openlattice.shuttle.logger.error(
                            "Destination " + associationDefinition.dstAlias
                                    + " cannot be found to construct association " + associationDefinition.alias
                    )
                }

                if (!wasCreated.containsKey(associationDefinition.srcAlias)) {
                    com.openlattice.shuttle.logger.error(
                            ("Source " + associationDefinition.srcAlias
                                    + " cannot be found to construct association " + associationDefinition.alias)
                    )
                }
                if ((wasCreated[associationDefinition.srcAlias] && wasCreated[associationDefinition.dstAlias])) {

                    val entitySetId = entitySetIdCache
                            .getUnchecked(associationDefinition.entitySetName)
                    val properties = HashMap<UUID, Set<Any>>()

                    for (propertyDefinition in associationDefinition.properties) {
                        val propertyValue = propertyDefinition.propertyValue.apply(row)
                        if (propertyValue != null) {
                            val propertyId = propertyIdsCache
                                    .getUnchecked(propertyDefinition.fullQualifiedName)
                            if (propertyValue is Iterable<*>) {
                                properties
                                        .putAll(
                                                ImmutableMap
                                                        .of(
                                                                propertyId,
                                                                Sets.newHashSet<Any>((propertyValue as Iterable<*>)!!)
                                                        )
                                        )
                            } else {
                                (properties as java.util.Map<UUID, Set<Any>>).computeIfAbsent(
                                        propertyId
                                ) { ptId -> HashSet() }
                                        .add(propertyValue)
                            }
                        }
                    }

                    val entityId = associationDefinition.generator
                            .map { g -> g.apply(row) }
                            .orElseGet {
                                generateDefaultEntityId(
                                        keyCache.getUnchecked(associationDefinition.entitySetName),
                                        properties
                                )
                            }

                    if (StringUtils.isNotBlank(entityId)) {
                        val key = EntityKey(entitySetId, entityId)
                        val src = aliasesToEntityKey[associationDefinition.srcAlias]
                        val dst = aliasesToEntityKey[associationDefinition.dstAlias]
                        associations.add(Association(key, src, dst, properties))
                    }
                }

                MissionControl.signal()
            }

            BulkDataCreation(
                    entities,
                    associations
            )
        }
                .reduce { a: BulkDataCreation, b: BulkDataCreation ->

                    a.associations.addAll(b.associations)
                    a.entities.addAll(b.entities)

                    if (a.associations.size > 100000 || a.entities.size > 100000) {
                        val entityKeys = a.entities.stream().map { entity -> entity.key }
                                .collect<Set<EntityKey>, Any>(Collectors.toSet())
                        entityKeys.addAll(a.associations.stream().map { association -> association.key }
                                                  .collect<Set<EntityKey>, Any>(
                                                          Collectors.toSet()
                                                  ))
                        val bulkEntitySetIds = dataApi.getEntityKeyIds(entityKeys)
                        sendDataToDataSink(a, bulkEntitySetIds, dataApi, s3Api)
                        return@payload
                                .parallel()
                                .map(row -> {
                            EdmApi edmApi;

                            try {
                                edmApi = this.apiClient.edmApi;
                            } catch (ExecutionException e) {
                                com.openlattice.shuttle.logger.error("Failed to retrieve apis.");
                                throw new IllegalStateException ("Unable to retrieve APIs for execution");
                            }

                            initializeEdmCaches(edmApi);

                            Map<String, EntityKey> aliasesToEntityKey = new HashMap<>();
                            Set<Entity> entities = Sets . newHashSet ();
                            Set<Association> associations = Sets . newHashSet ();
                            Map<String, Boolean> wasCreated = new HashMap<>();

                            if (flight.condition.isPresent) {
                                Object out = flight . valueMapper . apply (row);
                                if (!((Boolean) out).booleanValue()) {
                                    return new BulkDataCreation (entities, associations);
                                }
                            }

                            for (EntityDefinition entityDefinition : flight.getEntities()) {

                            Boolean condition = true;
                            if (entityDefinition.condition.isPresent()) {
                                Object out = entityDefinition . valueMapper . apply (row);
                                if (!(Boolean) out) {
                                    condition = false;
                                }
                            }

                            UUID entitySetId = entitySetIdCache . getUnchecked (entityDefinition.getEntitySetName());
                            Map<UUID, Set<Object>> properties = new HashMap<>();

                            if (!seenEntitySetIds.contains(entitySetId)) {
                                storageDestByProperty
                                        .putAll(
                                                dataApi.getPropertyTypesForEntitySet(entitySetId).entrySet().stream()
                                                        .filter(entry -> {
                                    if (entry.getValue().getDatatype()
                                                    .equals(EdmPrimitiveTypeKind.Binary)) {
                                        storageDestByProperty[entry.getKey()] = StorageDestination.AWS;
                                        return false;
                                    }
                                    return true;
                                } ).collect(Collectors
                                .toMap(entry -> entry.getKey(),
                                entry -> StorageDestination.POSTGRES)));
                                seenEntitySetIds.add(entitySetId);
                            }

                            for (PropertyDefinition propertyDefinition : entityDefinition.getProperties()) {
                            Object propertyValue = propertyDefinition . getPropertyValue ().apply(row);
                            if (propertyValue != null) {
                                String stringProp = propertyValue . toString ();
                                if (!StringUtils.isBlank(stringProp)) {
                                    UUID propertyId = propertyIdsCache
                                            .getUnchecked(propertyDefinition.getFullQualifiedName());
                                    if (propertyValue instanceof Iterable) {
                                        properties
                                                .putAll(
                                                        ImmutableMap.of(
                                                                propertyId,
                                                                Sets.newHashSet((Iterable<?>) propertyValue)
                                                        )
                                                );
                                    } else {
                                        properties.computeIfAbsent(propertyId, ptId -> new HashSet<>())
                                        .add(propertyValue);
                                    }
                                }
                            }
                        }

                            /*
                                     * For entityId generation to work correctly it is very important that Stream remain ordered. Ordered !=
                                     * sequential vs parallel.
                                     */

                            String entityId =(entityDefinition.getGenerator().isPresent())
                            ? entityDefinition.getGenerator().get().apply(row)
                            : generateDefaultEntityId(
                                keyCache.getUnchecked(entityDefinition.getEntitySetName()),
                                properties
                        );

                            if (StringUtils.isNotBlank(entityId) & condition & properties.size() > 0) {
                            EntityKey key = new EntityKey(entitySetId, entityId);
                            aliasesToEntityKey.put(entityDefinition.getAlias(), key);
                            entities.add(new Entity (key, properties) );
                            wasCreated.put(entityDefinition.getAlias(), true);
                        } else {
                            wasCreated.put(entityDefinition.getAlias(), false);
                        }

                            MissionControl.signal();
                        }

                            for (AssociationDefinition associationDefinition : flight.getAssociations()) {

                            if (associationDefinition.condition.isPresent()) {
                                Object out = associationDefinition . valueMapper . apply (row);
                                if (!(Boolean) out) {
                                    continue;
                                }
                            }

                            if (!wasCreated.containsKey(associationDefinition.getDstAlias())) {
                                com.openlattice.shuttle.logger.error(
                                        "Destination " + associationDefinition.getDstAlias()
                                                + " cannot be found to construct association " + associationDefinition.getAlias()
                                );
                            }

                            if (!wasCreated.containsKey(associationDefinition.getSrcAlias())) {
                                com.openlattice.shuttle.logger.error(
                                        "Source " + associationDefinition.getSrcAlias()
                                                + " cannot be found to construct association " + associationDefinition.getAlias()
                                );
                            }
                            if (wasCreated.get(associationDefinition.getSrcAlias())
                                    && wasCreated.get(associationDefinition.getDstAlias())) {

                                UUID entitySetId = entitySetIdCache
                                        .getUnchecked(associationDefinition.getEntitySetName());
                                Map<UUID, Set<Object>> properties = new HashMap<>();

                                for (PropertyDefinition propertyDefinition : associationDefinition.getProperties()) {
                                    Object propertyValue = propertyDefinition . getPropertyValue ().apply(row);
                                    if (propertyValue != null) {
                                        var propertyId = propertyIdsCache
                                                .getUnchecked(propertyDefinition.getFullQualifiedName());
                                        if (propertyValue instanceof Iterable) {
                                            properties
                                                    .putAll(
                                                            ImmutableMap
                                                                    .of(
                                                                            propertyId,
                                                                            Sets.newHashSet((Iterable<?>) propertyValue)
                                                                    )
                                                    );
                                        } else {
                                            properties.computeIfAbsent(propertyId, ptId -> new HashSet<>())
                                            .add(propertyValue);
                                        }
                                    }
                                }

                                String entityId = associationDefinition . getGenerator ()
                                        .map(g -> g.apply(row))
                                .orElseGet(() ->
                                generateDefaultEntityId(
                                        keyCache.getUnchecked(associationDefinition.getEntitySetName()),
                                        properties
                                ) );

                                if (StringUtils.isNotBlank(entityId)) {
                                    EntityKey key = new EntityKey(entitySetId, entityId);
                                    EntityKey src = aliasesToEntityKey . get (associationDefinition.getSrcAlias());
                                    EntityKey dst = aliasesToEntityKey . get (associationDefinition.getDstAlias());
                                    associations.add(new Association (key, src, dst, properties) );
                                }
                            }

                            MissionControl.signal();
                        }

                            return new BulkDataCreation (entities,
                            associations );
                        } )
                        .reduce BulkDataCreation (HashSet<Entity>(),
                        HashSet<Association>())
                    }

                    a
                }

        remaining.ifPresent { r ->
            val entityKeys = r.entities.stream().map { entity -> entity.key }
                    .collect<Set<EntityKey>, Any>(Collectors.toSet())
            entityKeys.addAll(
                    r.associations.stream().map { association -> association.key }.collect<Set<EntityKey>, Any>(
                            Collectors.toSet()
                    )
            )
            val bulkEntitySetIds = dataApi.getEntityKeyIds(entityKeys)
            sendDataToDataSink(r, bulkEntitySetIds, dataApi, s3Api)
        }
    }

    private fun ensureValidIntegration(flightsToPayloads: Map<Flight, Payload>) {
        flightsToPayloads.keys.forEach { flight ->

            flight
                    .entities
                    .forEach { entityDefinition ->
                        val entitySetId = entitySetIdCache.getUnchecked(entityDefinition.getEntitySetName())
                        assertPropertiesMatchEdm(
                                entityDefinition.getEntitySetName(),
                                entitySetId,
                                entityDefinition.getProperties(),
                                edmApi
                        )
                    }

            flight
                    .associations
                    .forEach { associationDefinition ->
                        val entitySetId = entitySetIdCache.getUnchecked(associationDefinition.getEntitySetName())
                        assertPropertiesMatchEdm(
                                associationDefinition.getEntitySetName(),
                                entitySetId,
                                associationDefinition.getProperties(),
                                edmApi
                        )
                    }
        }
    }

    private fun assertPropertiesMatchEdm(
            entitySetName: String,
            entitySetId: UUID,
            properties: Collection<PropertyDefinition>,
            edmApi: EdmApi
    ) {

        val propertyFqns = properties.map(PropertyDefinition::getFullQualifiedName).toSet()
        val entitySetPropertyFqns = edmApi
                .getEntityType(edmApi.getEntitySet(entitySetId).entityTypeId).properties
                .map { id -> edmApi.getPropertyType(id).type }
                .toSet()
        val illegalFqns = Sets.filter(propertyFqns) { fqn -> !entitySetPropertyFqns.contains(fqn) }
        if (!illegalFqns.isEmpty()) {
            illegalFqns.forEach { fqn ->
                logger.error(
                        "Entity set {} does not contain any property type with FQN: {}",
                        entitySetName,
                        fqn.toString()
                )
            }
            throw NullPointerException("Illegal property types defined for entity set $entitySetName")
        }
    }

}