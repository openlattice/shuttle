package com.openlattice.shuttle.control

import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.mapstores.TestDataFactory
import com.openlattice.shuttle.Flight
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 * @param sql the sql query to be used to pull cleaned data from postgres
 * @param source details regarding the source of the data
 * @param sourcePrimaryKeyColumns the columns that are primary keys in the cleaned data
 * @param environment the retrofit environment (e.g. production, local)
 * @param defaultStorage ?????
 * @param s3bucket the url of the s3bucket to be used
 * @param flight stringified version of a flight yaml
 * @param contacts the set of email addresses of those responsible for the integration
 * @param recurring boolean denoting if the integration is recurring
 * @param start ??? datetime in ms of first integration?
 * @param period ??? how often it gets rerun?
 */
data class Integration(
        val sql: String,
        val source: Properties,
        val sourcePrimaryKeyColumns: List<String> = listOf(),
        val environment: RetrofitFactory.Environment,
        val defaultStorage: StorageDestination,
        val s3bucket: String,
        val flight: Flight,
        val contacts: Set<String>,
        val recurring: Boolean,
        val start: Long,
        val period: Long
) {
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