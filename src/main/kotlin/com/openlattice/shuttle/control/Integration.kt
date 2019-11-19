package com.openlattice.shuttle.control

import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.StorageDestination
import com.openlattice.shuttle.Flight
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class Integration(
        val sql: String,
        val source: Properties,
        val sourcePrimaryKeyColumns: List<String> = listOf(),
        val environment: RetrofitFactory.Environment,
        val defaultStorage: StorageDestination,
        val s3bucket: String,
        val flight: Flight,
        val recurring: Boolean,
        val start: Long,
        val period: Long
)