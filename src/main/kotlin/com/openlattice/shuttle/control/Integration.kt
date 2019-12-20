package com.openlattice.shuttle.control

import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.integration.StorageDestination
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.Flight
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 *
 * Represents a data integration, including all fields required to run the integration
 *

 * @param environment the retrofit environment (e.g. production, local)
 * @param defaultStorage ?????
 * @param s3bucket the url of the s3bucket to be used
 * @param contacts the set of email addresses of those responsible for the integration
 * @param recurring boolean denoting if the integration is recurring
 * @param start ??? datetime in ms of first integration?
 * @param period ??? how often it gets run?
 * @param flightPlanParameters a map from [Flight] name to [FlightPlanParameters]
 */
data class Integration(
        @JsonProperty(SerializationConstants.ENVIRONMENT) var environment: RetrofitFactory.Environment,
        @JsonProperty(SerializationConstants.DEFAULT_STORAGE) var defaultStorage: StorageDestination,
        @JsonProperty(SerializationConstants.S3_BUCKET) var s3bucket: String,
        @JsonProperty(SerializationConstants.CONTACTS) var contacts: Set<String>,
        @JsonProperty(SerializationConstants.ENTITY_SET_ID) var logEntitySetId: Optional<UUID>,
        @JsonProperty(SerializationConstants.RECURRING) var recurring: Boolean,
        @JsonProperty(SerializationConstants.START) var start: Long,
        @JsonProperty(SerializationConstants.PERIOD) var period: Long,
        @JsonProperty(SerializationConstants.FLIGHT_PLAN_PARAMETERS) var flightPlanParameters: MutableMap<String, FlightPlanParameters>
) {

    companion object {

        @JvmStatic
        fun testData(): Integration {
            val environment = RetrofitFactory.Environment.LOCAL
            val defaultStorage = StorageDestination.POSTGRES
            val s3bucket = TestDataFactory.random(10)
            val contacts = setOf<String>(TestDataFactory.random(5))
            val recurring = true
            val start = 1000L
            val period = 5L
            return Integration(
                    environment,
                    defaultStorage,
                    s3bucket,
                    contacts,
                    Optional.empty(),
                    recurring,
                    start,
                    period,
                    mutableMapOf(TestDataFactory.randomAlphanumeric(5) to FlightPlanParameters.testData())
            )
        }

    }

}