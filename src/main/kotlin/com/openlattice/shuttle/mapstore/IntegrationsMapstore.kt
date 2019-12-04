package com.openlattice.shuttle.mapstore

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.module.kotlin.readValue
import com.hazelcast.config.MapConfig
import com.hazelcast.config.MapStoreConfig
import com.openlattice.hazelcast.HazelcastMap
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.postgres.PostgresColumn.INTEGRATION
import com.openlattice.postgres.PostgresColumn.NAME
import com.openlattice.postgres.PostgresColumnDefinition
import com.openlattice.postgres.PostgresTable.INTEGRATIONS
import com.openlattice.postgres.mapstores.AbstractBasePostgresMapstore
import com.openlattice.shuttle.control.Integration
import com.zaxxer.hikari.HikariDataSource
import java.sql.PreparedStatement
import java.sql.ResultSet

class IntegrationsMapstore(hds: HikariDataSource) : AbstractBasePostgresMapstore<String, Integration>(
        HazelcastMap.INTEGRATIONS.name, INTEGRATIONS, hds
) {
    private val mapper = ObjectMappers.newJsonMapper()

    override fun initValueColumns(): List<PostgresColumnDefinition> {
        return listOf(INTEGRATION)
    }

    override fun bind(ps: PreparedStatement, key: String, value: Integration) {
        val offset = bind(ps, key, 1)
        val integrationJson = mapper.writeValueAsString(value)
        ps.setObject(offset, integrationJson)
    }

    override fun bind(ps: PreparedStatement, key: String, offset: Int): Int {
        ps.setObject(offset, key)
        return offset + 1
    }

    override fun generateTestKey(): String {
        return TestDataFactory.randomAlphanumeric(5)
    }

    override fun generateTestValue(): Integration {
        return Integration.testData()
    }

    override fun getMapConfig(): MapConfig {
        return super.getMapConfig()
    }

    override fun getMapStoreConfig(): MapStoreConfig {
        return super.getMapStoreConfig()
    }

    override fun mapToKey(rs: ResultSet): String {
        return rs.getString(NAME.name)
    }

    override fun mapToValue(rs: ResultSet): Integration {
        return mapper.readValue(rs.getString(INTEGRATIONS.name))
    }
}