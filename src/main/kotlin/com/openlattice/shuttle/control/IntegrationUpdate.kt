package com.openlattice.shuttle.control

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.integration.StorageDestination
import com.openlattice.shuttle.Flight
import java.util.*

class IntegrationUpdate(
    @JsonProperty(SerializationConstants.SQL) val sql: Optional<String>,
    @JsonProperty(SerializationConstants.SRC) val source: Optional<Properties>,
    @JsonProperty(SerializationConstants.SRC_PKEY_COLUMNS) val sourcePrimaryKeyColumns: Optional<List<String>>,
    @JsonProperty(SerializationConstants.ENVIRONMENT) val environment: Optional<RetrofitFactory.Environment>,
    @JsonProperty(SerializationConstants.DEFAULT_STORAGE) val defaultStorage: Optional<StorageDestination>,
    @JsonProperty(SerializationConstants.S3_BUCKET) val s3bucket: Optional<String>,
    @JsonProperty(SerializationConstants.PATH) val flightFilePath: Optional<String>,
    @JsonProperty(SerializationConstants.TAGS) val tags: Optional<Set<String>>,
    @JsonProperty(SerializationConstants.CONTACTS) val contacts: Optional<Set<String>>,
    @JsonProperty(SerializationConstants.RECURRING) val recurring: Optional<Boolean>,
    @JsonProperty(SerializationConstants.START) val start: Optional<Long>,
    @JsonProperty(SerializationConstants.PERIOD) val period: Optional<Long>
)