package com.openlattice.shuttle.logs

import com.fasterxml.jackson.annotation.JsonProperty
import com.kryptnostic.rhizome.configuration.annotation.ReloadableConfiguration

@ReloadableConfiguration(uri = "blackbox.yaml")
data class Blackbox (
    @JsonProperty("entity-type") val entityTypeFqn: String,
    @JsonProperty("fqns") val fqns: Map<BlackboxProperty, String>,
    @JsonProperty("enabled") val enabled: Boolean = true
) {
    companion object {
        @JvmStatic
        fun empty(): Blackbox {
            return Blackbox("", emptyMap(), false)
        }
    }
}