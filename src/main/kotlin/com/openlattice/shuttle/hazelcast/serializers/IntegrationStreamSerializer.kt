package com.openlattice.shuttle.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.OptionalStreamSerializers
import com.openlattice.hazelcast.serializers.UUIDStreamSerializer
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.Integration
import org.springframework.stereotype.Component
import java.util.*

@Component
class IntegrationStreamSerializer : SelfRegisteringStreamSerializer<Integration> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()
        private val environments = RetrofitFactory.Environment.values()
        private val storageDestinations = StorageDestination.values()
        fun serialize(output: ObjectDataOutput, obj: Integration) {
            output.writeUTF(obj.sql)

            output.writeUTFArray(obj.source.keys.map { it as String }.toTypedArray())
            output.writeUTFArray(obj.source.values.map { it as String }.toTypedArray())

            output.writeUTFArray(obj.sourcePrimaryKeyColumns.toTypedArray())
            output.writeInt(obj.environment.ordinal)
            output.writeInt(obj.defaultStorage.ordinal)
            output.writeUTF(obj.s3bucket)

            if (obj.flightFilePath != null) {
                output.writeBoolean(true)
                output.writeUTF(obj.flightFilePath)
            } else {
                output.writeBoolean(false)
            }

            val flightJson = mapper.writeValueAsString(obj.flight)
            output.writeUTF(flightJson)

            output.writeUTFArray(obj.contacts.toTypedArray())
            OptionalStreamSerializers.serialize(output, obj.logEntitySetId, UUIDStreamSerializer::serialize)
            output.writeBoolean(obj.recurring)
            output.writeLong(obj.start)
            output.writeLong(obj.period)
        }

        fun deserialize(input: ObjectDataInput): Integration {
            val sql = input.readUTF()

            val sourceKeys = input.readUTFArray().toList()
            val sourceValues = input.readUTFArray().toList()
            val sourceMap = sourceKeys.zip(sourceValues) { key, value -> key to value }.toMap()
            val source = Properties()
            source.putAll(sourceMap)

            val srcPkeyCols = input.readUTFArray().toList()
            val environment = environments[input.readInt()]
            val defaultStorage = storageDestinations[input.readInt()]
            val s3bucket = input.readUTF()

            var flightFilePath: String? = null
            val hasFlightFilePath = input.readBoolean()
            if (hasFlightFilePath) flightFilePath = input.readUTF()

            val flightJson = input.readUTF()
            val flight = mapper.readValue(flightJson, Flight::class.java)

            val contacts = input.readUTFArray().toSet()
            val logEntitySetId = OptionalStreamSerializers.deserialize(input, UUIDStreamSerializer::deserialize)
            val recurring = input.readBoolean()
            val start = input.readLong()
            val period = input.readLong()
            return Integration(
                    sql,
                    source,
                    srcPkeyCols,
                    environment,
                    defaultStorage,
                    s3bucket,
                    flightFilePath,
                    flight,
                    contacts,
                    logEntitySetId,
                    recurring,
                    start,
                    period
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

}