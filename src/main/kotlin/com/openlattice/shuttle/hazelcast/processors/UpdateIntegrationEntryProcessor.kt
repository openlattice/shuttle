package com.openlattice.shuttle.hazelcast.processors
import com.dataloom.mappers.ObjectMappers
import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationUpdate
import java.net.URL

class UpdateIntegrationEntryProcessor(val update: IntegrationUpdate) :
        AbstractRhizomeEntryProcessor<String, Integration, Integration>() {

    companion object {
        private val mapper = ObjectMappers.getYamlMapper()
    }

    override fun process(entry: MutableMap.MutableEntry<String, Integration>): Integration {
        val integration = entry.value

        update.sql.ifPresent { integration.sql = it }

        update.source.ifPresent { integration.source = it }

        update.sourcePrimaryKeyColumns.ifPresent { integration.sourcePrimaryKeyColumns = it }

        update.environment.ifPresent { integration.environment = it }

        update.defaultStorage.ifPresent { integration.defaultStorage = it }

        update.s3bucket.ifPresent { integration.s3bucket = it }

        update.flightFilePath.ifPresent {
            integration.flightFilePath = it
            val updatedFlight = mapper.readValue(URL(it), Flight::class.java)
            integration.flight = updatedFlight
        }

        update.contacts.ifPresent { integration.contacts = it }

        update.recurring.ifPresent { integration.recurring = it }

        update.start.ifPresent { integration.start = it }

        update.period.ifPresent { integration.period = it }

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