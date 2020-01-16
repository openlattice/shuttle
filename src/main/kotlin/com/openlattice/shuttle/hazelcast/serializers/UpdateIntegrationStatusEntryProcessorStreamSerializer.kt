package com.openlattice.shuttle.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.shuttle.hazelcast.processors.UpdateIntegrationStatusEntryProcessor
import org.springframework.stereotype.Component

@Component
class UpdateIntegrationStatusEntryProcessorStreamSerializer : SelfRegisteringStreamSerializer<UpdateIntegrationStatusEntryProcessor> {
    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.UPDATE_INTEGRATION_STATUS_EP.ordinal
    }

    override fun getClazz(): Class<out UpdateIntegrationStatusEntryProcessor> {
        return UpdateIntegrationStatusEntryProcessor::class.java
    }

    override fun read(`in`: ObjectDataInput): UpdateIntegrationStatusEntryProcessor {
        return UpdateIntegrationStatusEntryProcessor(IntegrationStatusStreamSerializer.deserialize(`in`))
    }

    override fun write(out: ObjectDataOutput, `object`: UpdateIntegrationStatusEntryProcessor) {
        IntegrationStatusStreamSerializer.serialize(out, `object`.status)
    }
}