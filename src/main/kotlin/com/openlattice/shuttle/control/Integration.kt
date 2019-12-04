package com.openlattice.shuttle.control

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.annotation.JsonProperty
import com.openlattice.client.RetrofitFactory
import com.openlattice.client.serialization.SerializationConstants
import com.openlattice.data.integration.StorageDestination
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.Flight
import java.io.File
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
 * @param flight Flight object
 * @param contacts the set of email addresses of those responsible for the integration
 * @param recurring boolean denoting if the integration is recurring
 * @param start ??? datetime in ms of first integration?
 * @param period ??? how often it gets run?
 */
data class Integration(
        @JsonProperty(SerializationConstants.SQL) val sql: String,
        @JsonProperty(SerializationConstants.SRC) val source: Properties,
        @JsonProperty(SerializationConstants.SRC_PKEY_COLUMNS) val sourcePrimaryKeyColumns: List<String> = listOf(),
        @JsonProperty(SerializationConstants.ENVIRONMENT) val environment: RetrofitFactory.Environment,
        @JsonProperty(SerializationConstants.DEFAULT_STORAGE) val defaultStorage: StorageDestination,
        @JsonProperty(SerializationConstants.S3_BUCKET) val s3bucket: String,
        @JsonProperty(SerializationConstants.FLIGHT) val flight: Flight,
        @JsonProperty(SerializationConstants.CONTACTS) val contacts: Set<String>,
        @JsonProperty(SerializationConstants.RECURRING) val recurring: Boolean,
        @JsonProperty(SerializationConstants.START) val start: Long,
        @JsonProperty(SerializationConstants.PERIOD) val period: Long
) {
    constructor(sql: String,
                source: Properties,
                sourcePrimaryKeyColumns: List<String> = listOf(),
                environment: RetrofitFactory.Environment,
                defaultStorage: StorageDestination,
                s3bucket: String,
                flightFilePath: String,
                contacts: Set<String>,
                recurring: Boolean,
                start: Long,
                period: Long) : this(
            sql,
            source,
            sourcePrimaryKeyColumns,
            environment,
            defaultStorage,
            s3bucket,
            ObjectMappers.getYamlMapper().readValue(File(flightFilePath), Flight::class.java),
            contacts,
            recurring,
            start,
            period
    )

    companion object {

        @JvmStatic
        fun testData(): Integration {
            val sql = TestDataFactory.random(5)
            val source = Properties()
            val pkeyCols = listOf<String>()
            val environment = RetrofitFactory.Environment.PROD_INTEGRATION
            val defaultStorage = StorageDestination.POSTGRES
            val s3bucket = TestDataFactory.random(10)
            val flight = Flight(emptyMap(), Optional.empty(), emptyMap())
            val contacts = setOf<String>(TestDataFactory.random(5))
            val recurring = true
            val start = 1000L
            val period = 5L
            return Integration(sql, source, pkeyCols, environment,
                    defaultStorage, s3bucket, flight, contacts, recurring, start, period)
        }

    }
}