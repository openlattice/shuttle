package com.openlattice.shuttle.control

import com.hazelcast.core.HazelcastInstance
import com.openlattice.edm.EntitySet
import com.openlattice.edm.type.EntityType
import com.openlattice.edm.type.PropertyType
import com.openlattice.hazelcast.HazelcastMap
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.slf4j.LoggerFactory
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
class IntegrationsService(hazelcastInstance: HazelcastInstance) {
    companion object {
        private val logger = LoggerFactory.getLogger(IntegrationsService::class.java)
    }

    private val entitySets = HazelcastMap.ENTITY_SETS.getMap( hazelcastInstance )
    private val entityTypes = HazelcastMap.ENTITY_TYPES.getMap( hazelcastInstance )
    private val propertyTypes = HazelcastMap.PROPERTY_TYPES.getMap( hazelcastInstance )
    fun runIntegration(integration: Integration) {
//        val payload = JdbcPayload(getDatasource(integration.source), integration.sql)
        val ptValues = propertyTypes.values
//        Shuttle(
//                mapOf(integration.flight to payload),
//                entitySets.values.associateBy { it.name },
//                entityTypes.values.associateBy { it.id },
//                ptValues.associateBy { it.type },
//                ptValues.associateBy { it.id },
//                mapOf(StorageDestination),
//
//        )
    }

//    fun getJdbcPayload(hds: HikariDataSource, sql: String): JdbcPayload {
//        return JdbcPayload(hds, sql)
//    }

    fun getDatasource(properties: Properties): HikariDataSource {
        val hc = HikariConfig(properties)
        logger.info("JDBC URL = {}", hc.getJdbcUrl())
        return HikariDataSource(hc)
    }

}