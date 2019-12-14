package com.openlattice.shuttle.control

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.integration.StorageDestination
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.Flight
import java.io.File
import java.net.URI
import java.net.URL
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 *
 * Represents a data integration, including all fields required to run the integration
 *
 * @param sql the sql query to be used to pull cleaned data from postgres
 * @param source postgres data source for pulling clean data
 * @param sourcePrimaryKeyColumns the columns that are primary keys in the cleaned data
 * @param environment the retrofit environment (e.g. production, local)
 * @param defaultStorage ?????
 * @param s3bucket the url of the s3bucket to be used
 * @param flightFilePath the path to the flight yaml. If you would like the flight to
 * be reloadable from this path, please store flight yaml on github and provide the path
 * to it (i.e. https://raw.githubusercontent.com/pathToFlight.yaml)
 * @param flight Flight object
 * @param contacts the set of email addresses of those responsible for the integration
 * @param recurring boolean denoting if the integration is recurring
 * @param start ??? datetime in ms of first integration?
 * @param period ??? how often it gets run?
 */
data class Integration(
        @JsonProperty(SerializationConstants.SQL) var sql: String,
        @JsonProperty(SerializationConstants.SRC) var source: Properties,
        @JsonProperty(SerializationConstants.SRC_PKEY_COLUMNS) var sourcePrimaryKeyColumns: List<String> = listOf(),
        @JsonProperty(SerializationConstants.ENVIRONMENT) var environment: RetrofitFactory.Environment,
        @JsonProperty(SerializationConstants.DEFAULT_STORAGE) var defaultStorage: StorageDestination,
        @JsonProperty(SerializationConstants.S3_BUCKET) var s3bucket: String,
        @JsonProperty(SerializationConstants.PATH) var flightFilePath: String?,
        @JsonProperty(SerializationConstants.FLIGHT) var flight: Flight?,
        @JsonProperty(SerializationConstants.CONTACTS) var contacts: Set<String>,
        @JsonProperty(SerializationConstants.RECURRING) var recurring: Boolean,
        @JsonProperty(SerializationConstants.START) var start: Long,
        @JsonProperty(SerializationConstants.PERIOD) var period: Long
) {
    init {
        if (flightFilePath == null) check(flight != null) {"Either flight or flightFilePath must not be null"}
        if (flight == null) check(flightFilePath != null) {"Either flight or flightFilePath must not be null"}
        if (flightFilePath != null && flight == null) {
            this.flight = ObjectMappers.getYamlMapper().readValue(URL(flightFilePath!!), Flight::class.java)
        }
    }

    companion object {

        @JvmStatic
        fun testData(): Integration {
            val sql = TestDataFactory.random(5)
            val source = Properties()
            val pkeyCols = listOf<String>()
            val environment = RetrofitFactory.Environment.PROD_INTEGRATION
            val defaultStorage = StorageDestination.POSTGRES
            val s3bucket = TestDataFactory.random(10)
            val flight = Flight(emptyMap(), Optional.empty(), Optional.of(emptyMap()), Optional.of(TestDataFactory.random(5)))
            val contacts = setOf<String>(TestDataFactory.random(5))
            val recurring = true
            val start = 1000L
            val period = 5L
            return Integration(sql, source, pkeyCols, environment,
                    defaultStorage, s3bucket, null, flight, contacts, recurring, start, period)
        }

    }

}