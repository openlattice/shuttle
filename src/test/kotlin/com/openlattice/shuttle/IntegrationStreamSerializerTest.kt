package com.openlattice.shuttle

import com.kryptnostic.rhizome.hazelcast.serializers.AbstractStreamSerializerTest
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.serializers.IntegrationStreamSerializer

class IntegrationStreamSerializerTest : AbstractStreamSerializerTest<IntegrationStreamSerializer, Integration>() {
    override fun createSerializer(): IntegrationStreamSerializer {
        return IntegrationStreamSerializer()
    }

    override fun createInput(): Integration {
        return Integration.testData()
    }
}