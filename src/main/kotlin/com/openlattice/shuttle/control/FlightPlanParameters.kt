package com.openlattice.shuttle.control

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.Flight
import java.net.URL
import java.util.*

/**
 * Represents the parameters required to create a flight plan (i.e. a Map<Flight, Payload>)
 *
 * @param sql the sql query to be used to pull cleaned data from postgres
 * @param source postgres data source for pulling clean data
 * @param sourcePrimaryKeyColumns the columns that are primary keys in the cleaned data
 * @param flightFilePath the path to the flight yaml. If you would like the flight to
 * be reloadable from this path, please store flight yaml on github and provide the path
 * to it (i.e. https://raw.githubusercontent.com/pathToFlight.yaml)
 * @param flight Flight object
 */

data class FlightPlanParameters(
        @JsonProperty(SerializationConstants.SQL) var sql: String,
        @JsonProperty(SerializationConstants.SRC) var source: Properties,
        @JsonProperty(SerializationConstants.SRC_PKEY_COLUMNS) var sourcePrimaryKeyColumns: List<String> = listOf(),
        @JsonProperty(SerializationConstants.PATH) var flightFilePath: String?,
        @JsonProperty(SerializationConstants.FLIGHT) var flight: Flight?
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
        fun testData(): FlightPlanParameters {
            val sql = TestDataFactory.randomAlphanumeric(5)
            val source = Properties()
            val sourcePrimaryKeyColumns = listOf(TestDataFactory.randomAlphanumeric(5))
            val flight = Flight(emptyMap(), Optional.empty(), Optional.of(emptyMap()), Optional.of(TestDataFactory.random(5)), Optional.empty())
            return FlightPlanParameters(
                    sql,
                    source,
                    sourcePrimaryKeyColumns,
                    null,
                    flight
            )
        }
    }
}