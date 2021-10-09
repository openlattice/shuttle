package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import com.openlattice.shuttle.util.DataStoreType
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@JsonIgnoreProperties(value = ["enabled"])
data class PostgresConfiguration(
    @JsonProperty("config") val config: Properties
)

@ReloadableConfiguration(uri = "shuttle.yaml")
data class MissionParameters(
    @JsonProperty("postgres") val postgres: PostgresConfiguration,
    @JsonProperty("aurora") val aurora: PostgresConfiguration,
    @JsonProperty("alpr") val alpr: PostgresConfiguration
) {
    companion object {
        @JvmStatic
        fun empty(): MissionParameters {
            return MissionParameters(
                PostgresConfiguration(Properties()),
                PostgresConfiguration(Properties()),
                PostgresConfiguration(Properties())
            )
        }
    }

    fun getTargetHikariDataSource(targetDataStore: DataStoreType): HikariDataSource {
        return when (targetDataStore) {
            DataStoreType.ALPR -> HikariDataSource(HikariConfig(alpr.config))
            DataStoreType.AURORA -> HikariDataSource(HikariConfig(aurora.config))
            else -> HikariDataSource(HikariConfig(postgres.config))
        }
    }
}
