package com.openlattice.shuttle.hazelcast.serializers

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.KotlinOptionalStreamSerializers
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.control.FlightPlanParametersUpdate
import com.openlattice.shuttle.control.IntegrationUpdate
import org.springframework.stereotype.Component
import java.util.*

@Component
class IntegrationUpdateStreamSerializer : TestableSelfRegisteringStreamSerializer<IntegrationUpdate> {

    companion object {
        fun serialize(output: ObjectDataOutput, obj: IntegrationUpdate) {
            if (obj.environment.isPresent) {
                output.writeBoolean(true)
                EnvironmentStreamSerializer.serialize(output, obj.environment.get())
            } else {
                output.writeBoolean(false)
            }
            OptionalStreamSerializers.serialize(output, obj.s3bucket, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serializeSet(output, obj.contacts, ObjectDataOutput::writeUTF)
            OptionalStreamSerializers.serialize(output, obj.maxConnections, ObjectDataOutput::writeInt)
            OptionalStreamSerializers.serializeList(output, obj.callbackUrls, ObjectDataOutput::writeUTF)
            if (obj.flightPlanParameters.isPresent) {
                output.writeBoolean(true)
                val flightPlanParameters = obj.flightPlanParameters.get()
                output.writeInt(flightPlanParameters.size)
                output.writeUTFArray(flightPlanParameters.keys.map { it }.toTypedArray())
                flightPlanParameters.values.forEach {
                    FlightPlanParametersUpdateStreamSerializer.serialize(output, it)
                }
            } else {
                output.writeBoolean(false)
            }
        }

        fun deserialize(input: ObjectDataInput): IntegrationUpdate {
            val env = if (input.readBoolean()) {
                Optional.of(EnvironmentStreamSerializer.deserialize(input))
            } else {
                Optional.empty()
            }
            val s3bucket = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readUTF)
            val contacts = OptionalStreamSerializers.deserializeSet(input, ObjectDataInput::readUTF)
            val maxConnections = OptionalStreamSerializers.deserialize(input, ObjectDataInput::readInt)
            val callbackUrls = OptionalStreamSerializers.deserializeList(input, ObjectDataInput::readUTF)
            val flightPlanParameters = if (input.readBoolean()) {
                val size = input.readInt()
                val keys = input.readUTFArray()
                val vals = mutableListOf<FlightPlanParametersUpdate>()
                for (i in 0 until size) {
                    vals.add(FlightPlanParametersUpdateStreamSerializer.deserialize(input))
                }
                Optional.of(keys.zip(vals).toMap())
            } else {
                Optional.empty()
            }
            return IntegrationUpdate(
                    env,
                    s3bucket,
                    contacts,
                    maxConnections,
                    callbackUrls,
                    flightPlanParameters
            )
        }
    }

    override fun write(output: ObjectDataOutput, obj: IntegrationUpdate) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): IntegrationUpdate {
        return deserialize(input)
    }

    override fun getClazz(): Class<out IntegrationUpdate> {
        return IntegrationUpdate::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION_UPDATE.ordinal
    }

    override fun generateTestValue(): IntegrationUpdate {
        return IntegrationUpdate(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty())
    }

}