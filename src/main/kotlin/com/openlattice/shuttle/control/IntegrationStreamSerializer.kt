package com.openlattice.shuttle.control

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.kryptnostic.rhizome.pods.hazelcast.SelfRegisteringStreamSerializer
import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.hazelcast.StreamSerializerTypeIds

class IntegrationStreamSerializer : SelfRegisteringStreamSerializer<Integration> {

    companion object {
        private val environments = RetrofitFactory.Environment.values()
        private val storageDestinations = StorageDestination.values()
        fun serialize(output: ObjectDataOutput, obj: Integration) {
            output.writeUTF(obj.sql)
            //something custom for source
            output.writeUTFArray(obj.sourcePrimaryKeyColumns.toTypedArray())
            output.writeInt(obj.environment.ordinal)
            output.writeInt(obj.defaultStorage.ordinal)
            output.writeUTF(obj.s3bucket)
            //write the flight as json probs
            output.writeUTFArray(obj.contacts.toTypedArray())
            output.writeBoolean(obj.recurring)
            output.writeLong(obj.start)
            output.writeLong(obj.period)
        }

        fun deserialize(input: ObjectDataInput): Integration {
            val sql = input.readUTF()
            //source
            val srcPkeyCols = input.readUTFArray().toList()
            val environment = environments[input.readInt()]
            val defaultStorage = storageDestinations[input.readInt()]
            val s3bucket = input.readUTF()
            //flight?! :D
            val contacts = input.readUTFArray().toSet()
            val recurring = input.readBoolean()
            val start = input.readLong()
            val period = input.readLong()
            return Integration(
                    sql,
                    //src,
                    srcPkeyCols
                    environment,
                    defaultStorage,
                    s3bucket,
                    //flight,
                    contacts,
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
        StreamSerializerTypeIds.INTEGRATION
    }

}