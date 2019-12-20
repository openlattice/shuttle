package com.openlattice.shuttle.control

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import java.util.*

class FlightPlanParametersUpdate(
        @JsonProperty(SerializationConstants.SQL) var sql: Optional<String>,
        @JsonProperty(SerializationConstants.SRC) var source: Optional<Properties>,
        @JsonProperty(SerializationConstants.SRC_PKEY_COLUMNS) var sourcePrimaryKeyColumns: Optional<List<String>>,
        @JsonProperty(SerializationConstants.PATH) var flightFilePath: Optional<String>
)