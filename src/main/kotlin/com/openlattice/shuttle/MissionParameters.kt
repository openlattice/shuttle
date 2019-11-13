package com.openlattice.shuttle

import com.fasterxml.jackson.annotation.JsonProperty
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class PostgresConfiguration(@JsonProperty("enabled") val enabled: Boolean,
                                 @JsonProperty("config") val config: Properties)
data class MissionParameters(
        @JsonProperty("postgres") val postgres: PostgresConfiguration
)