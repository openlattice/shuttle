package com.openlattice.shuttle.hazelcast.processors

import com.kryptnostic.rhizome.hazelcast.processors.AbstractRhizomeEntryProcessor
import com.openlattice.shuttle.control.IntegrationJob
import com.openlattice.shuttle.control.IntegrationStatus
import java.util.*

class UpdateIntegrationStatusEntryProcessor(val status: IntegrationStatus) :
        AbstractRhizomeEntryProcessor<UUID, IntegrationJob, IntegrationJob>() {

    override fun process(entry: MutableMap.MutableEntry<UUID, IntegrationJob>): IntegrationJob {
        val job = entry.value
        job.integrationStatus = status
        entry.setValue(job)
        return job
    }
}