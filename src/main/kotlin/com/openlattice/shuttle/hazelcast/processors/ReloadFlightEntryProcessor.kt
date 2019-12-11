package com.openlattice.shuttle.hazelcast.processors

import com.dataloom.mappers.ObjectMappers
import com.google.common.base.Preconditions.checkState
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.Integration
import java.io.File

class ReloadFlightEntryProcessor : AbstractRhizomeEntryProcessor<String, Integration, Integration>() {

    companion object {
        private const val serialVersionUID = -6602384557982348L
        private val mapper = ObjectMappers.getYamlMapper()
    }

    override fun process(entry: MutableMap.MutableEntry<String, Integration>): Integration {
        val integration = entry.value
        val flightFilePath = integration.flightFilePath
        checkState(flightFilePath != null, "Integration with name ${entry.key} does not have a flight file path")
        val updatedFlight = mapper.readValue(File(integration.flightFilePath!!), Flight::class.java)
        integration.flight = updatedFlight
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