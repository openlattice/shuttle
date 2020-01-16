package com.openlattice.shuttle.control

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.integration.StorageDestination
import com.openlattice.shuttle.Flight
import java.util.*

data class IntegrationUpdate(
    @JsonProperty(SerializationConstants.ENVIRONMENT) val environment: Optional<RetrofitFactory.Environment>,
    @JsonProperty(SerializationConstants.S3_BUCKET) val s3bucket: Optional<String>,
    @JsonProperty(SerializationConstants.CONTACTS) val contacts: Optional<Set<String>>,
    @JsonProperty(SerializationConstants.CONNECTIONS) val maxConnections: Optional<Int>,
    @JsonProperty(SerializationConstants.CALLBACK) val callbackUrls: Optional<List<String>>,
    @JsonProperty(SerializationConstants.FLIGHT_PLAN_PARAMETERS) val flightPlanParameters: Optional<Map<String, FlightPlanParametersUpdate>>
)