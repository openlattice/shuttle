package com.openlattice.shuttle.control

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.mapstores.TestDataFactory
import java.net.URL
import java.util.*

data class IntegrationJob(
        @JsonProperty(SerializationConstants.NAME) val integrationName: String,
        @JsonProperty(SerializationConstants.STATUS) var integrationStatus: IntegrationStatus,
        @JsonProperty(SerializationConstants.URL) val callbackUrl: Optional<URL>
) {
    companion object {
        @JvmStatic
        fun testData(): IntegrationJob {
            return IntegrationJob(TestDataFactory.randomAlphanumeric(5), IntegrationStatus.IN_PROGRESS, Optional.empty())
        }
    }
}