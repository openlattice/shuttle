package com.openlattice.shuttle.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.hazelcast.serializers.UUIDStreamSerializer
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.FlightPlanParameters
import com.openlattice.shuttle.control.Integration
import org.springframework.stereotype.Component
import java.util.*

@Component
class IntegrationStreamSerializer : TestableSelfRegisteringStreamSerializer<Integration> {

    companion object {
        private val environments = RetrofitFactory.Environment.values()
        private val storageDestinations = StorageDestination.values()

        fun serialize(output: ObjectDataOutput, obj: Integration) {
            output.writeInt(obj.environment.ordinal)
            output.writeInt(obj.defaultStorage.ordinal)
            output.writeUTF(obj.s3bucket)
            output.writeUTFArray(obj.contacts.toTypedArray())
            OptionalStreamSerializers.serialize(output, obj.logEntitySetId, UUIDStreamSerializer::serialize)
            output.writeBoolean(obj.recurring)
            output.writeLong(obj.start)
            output.writeLong(obj.period)
            output.writeUTFArray(obj.flightPlanParameters.keys.toTypedArray())
            obj.flightPlanParameters.values.forEach { FlightPlanParametersStreamSerializer.serialize(output, it) }
        }

        fun deserialize(input: ObjectDataInput): Integration {
            val environment = environments[input.readInt()]
            val defaultStorage = storageDestinations[input.readInt()]
            val s3bucket = input.readUTF()
            val contacts = input.readUTFArray().toSet()
            val logEntitySetId = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializer::deserialize)
            val recurring = input.readBoolean()
            val start = input.readLong()
            val period = input.readLong()
            val flightPlanParamsKeys = input.readUTFArray()
            val flightPlanParamsValues = flightPlanParamsKeys.map{
                FlightPlanParametersStreamSerializer.deserialize(input)
            }.toList()
            val flightPlanParams = flightPlanParamsKeys.zip(flightPlanParamsValues).toMap().toMutableMap()
            return Integration(
                    environment,
                    defaultStorage,
                    s3bucket,
                    contacts,
                    logEntitySetId,
                    recurring,
                    start,
                    period,
                    flightPlanParams
            )
        }
    }

    override fun write(output: ObjectDataOutput, obj: Integration) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): Integration {
        return deserialize(input)
    }

    override fun getClazz(): Class<out Integration> {
        return Integration::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.INTEGRATION.ordinal
    }

    override fun generateTestValue(): Integration {
        return Integration.testData()
    }

}