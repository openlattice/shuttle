package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class PostgresConfiguration(
        @JsonProperty("enabled") val enabled: Boolean,
        @JsonProperty("config") val config: Properties
)

data class AuthConfiguration(
        @JsonProperty("credentials") val credentials: Properties
)

@ReloadableConfiguration(uri = "shuttle.yaml")
data class MissionParameters(
        @JsonProperty("postgres") val postgres: PostgresConfiguration,
        @JsonProperty("auth") val auth: AuthConfiguration

) {
    companion object {
        @JvmStatic
        fun empty(): MissionParameters {
            return MissionParameters(PostgresConfiguration(false, Properties()), AuthConfiguration(Properties()))
        }
    }
}
