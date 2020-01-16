package com.openlattice.shuttle.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.AbstractEnumSerializer
import com.openlattice.shuttle.control.IntegrationStatus

class IntegrationStatusStreamSerializer : AbstractEnumSerializer<IntegrationStatus>() {
    companion object {
        @JvmStatic
        fun serialize(output: ObjectDataOutput, obj: IntegrationStatus) = AbstractEnumSerializer.serialize(output, obj)
        @JvmStatic
        fun deserialize(input: ObjectDataInput): IntegrationStatus = deserialize(IntegrationStatus::class.java, input) as IntegrationStatus
    }

    override fun generateTestValue(): Enum<IntegrationStatus> {
        return IntegrationStatus.IN_PROGRESS
    }

    override fun getClazz(): Class<out Enum<IntegrationStatus>> {
        return IntegrationStatus::class.java
    }

    override fun getTypeId(): Int {
        StreamSerializerTypeIds.INTEGRATION_STATUS.ordinal
    }

}