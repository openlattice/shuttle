package com.openlattice.shuttle.hazelcast.processors

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions.checkState
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.Integration
import java.io.File
import java.net.URL

class ReloadFlightEntryProcessor : AbstractRhizomeEntryProcessor<String, Integration, Integration>() {

    companion object {
        private val mapper = ObjectMappers.getYamlMapper()
    }

    override fun process(entry: MutableMap.MutableEntry<String, Integration>): Integration {
        val integration = entry.value

        integration.flightPlanParameters.forEach {
            if (it.value.flightFilePath != null) {
                val updatedFlight = mapper.readValue(URL(it.value.flightFilePath!!), Flight::class.java)
                it.value.flight = updatedFlight
            }
        }
        entry.setValue(integration)
        return integration
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        return true
    }

    override fun hashCode(): Int {
        return javaClass.hashCode()
    }

}