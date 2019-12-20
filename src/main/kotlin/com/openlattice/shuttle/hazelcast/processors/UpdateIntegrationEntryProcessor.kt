package com.openlattice.shuttle.hazelcast.processors
import com.dataloom.mappers.ObjectMappers
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.datastore.services.EntitySetManager
import com.openlattice.edm.requests.MetadataUpdate
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.IntegrationService
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationUpdate
import java.lang.Exception
import java.net.URL
import java.util.*

class UpdateIntegrationEntryProcessor(val update: IntegrationUpdate) :
        AbstractRhizomeEntryProcessor<String, Integration, Integration>() {

    companion object {
        private val mapper = ObjectMappers.getYamlMapper()
    }

    override fun process(entry: MutableMap.MutableEntry<String, Integration>): Integration {
        val integration = entry.value

        update.environment.ifPresent { integration.environment = it }

        update.defaultStorage.ifPresent { integration.defaultStorage = it }

        update.s3bucket.ifPresent { integration.s3bucket = it }

        update.contacts.ifPresent { integration.contacts = it }

        update.recurring.ifPresent { integration.recurring = it }

        update.start.ifPresent { integration.start = it }

        update.period.ifPresent { integration.period = it }

        update.flightPlanParameters.ifPresent {
            val flightPlanParameters = integration.flightPlanParameters
            it.forEach { entry ->
                val currentFlightPlanParameter = flightPlanParameters.getValue(entry.key)
                val update = entry.value
                if (update.sql.isPresent) { currentFlightPlanParameter.sql = update.sql.get() }
                if (update.source.isPresent) { currentFlightPlanParameter.source = update.source.get() }
                if (update.sourcePrimaryKeyColumns.isPresent) { currentFlightPlanParameter.sourcePrimaryKeyColumns = update.sourcePrimaryKeyColumns.get() }
                if (update.flightFilePath.isPresent) {
                    val updatedFlightFilePath = update.flightFilePath.get()
                    currentFlightPlanParameter.flightFilePath = updatedFlightFilePath
                    currentFlightPlanParameter.flight = mapper.readValue(URL(updatedFlightFilePath), Flight::class.java)
                }
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