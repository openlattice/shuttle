package com.openlattice.shuttle.hazelcast.serializers

import com.dataloom.mappers.ObjectMappers
import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.openlattice.hazelcast.StreamSerializerTypeIds
import com.openlattice.hazelcast.serializers.TestableSelfRegisteringStreamSerializer
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.control.FlightPlanParameters
import org.springframework.stereotype.Component
import java.util.*

@Component
class FlightPlanParametersStreamSerializer : TestableSelfRegisteringStreamSerializer<FlightPlanParameters> {

    companion object {
        private val mapper = ObjectMappers.getJsonMapper()

        fun serialize(output: ObjectDataOutput, obj: FlightPlanParameters) {
            output.writeUTF(obj.sql)
            output.writeUTFArray(obj.source.keys.map { it as String }.toTypedArray())
            output.writeUTFArray(obj.source.values.map { it as String }.toTypedArray())
            output.writeUTFArray(obj.sourcePrimaryKeyColumns.toTypedArray())
            if (obj.flightFilePath != null) {
                output.writeBoolean(true)
                output.writeUTF(obj.flightFilePath!!)
            } else {
                output.writeBoolean(false)
            }

            val flightJson = mapper.writeValueAsString(obj.flight)
            output.writeUTF(flightJson)
        }

        fun deserialize(input: ObjectDataInput): FlightPlanParameters {
            val sql = input.readUTF()
            val sourceKeys = input.readUTFArray().toList()
            val sourceValues = input.readUTFArray().toList()
            val sourceMap = sourceKeys.zip(sourceValues) { key, value -> key to value }.toMap()
            val source = Properties()
            source.putAll(sourceMap)
            val srcPkeyCols = input.readUTFArray().toList()
            var flightFilePath: String? = null
            val hasFlightFilePath = input.readBoolean()
            if (hasFlightFilePath) flightFilePath = input.readUTF()
            val flightJson = input.readUTF()
            val flight = mapper.readValue(flightJson, Flight::class.java)
            return FlightPlanParameters(
                    sql,
                    source,
                    srcPkeyCols,
                    flightFilePath,
                    flight
            )
        }

    }

    override fun write(output: ObjectDataOutput, obj: FlightPlanParameters) {
        serialize(output, obj)
    }

    override fun read(input: ObjectDataInput): FlightPlanParameters {
        return deserialize(input)
    }

    override fun getClazz(): Class<out FlightPlanParameters> {
        return FlightPlanParameters::class.java
    }

    override fun getTypeId(): Int {
        return StreamSerializerTypeIds.FLIGHT_PLAN_PARAMETERS.ordinal
    }

    override fun generateTestValue(): FlightPlanParameters {
        return FlightPlanParameters.testData()
    }
}