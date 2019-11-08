package com.openlattice.shuttle.control

import com.openlattice.client.RetrofitFactory
import com.openlattice.data.integration.IntegrationDestination
import com.openlattice.shuttle.Flight
import com.openlattice.shuttle.config.IntegrationConfig
import java.util.*

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
data class Integration(
                  val source : Properties,
                  val sourcePrimaryKeyColumns : List<String> = listOf(),
                  val environment: RetrofitFactory.Environment,
                  val defaultDestination: String,
                  val s3bucket: String,
                  val flight: Flight,
                  val recurring : Boolean,
                  val start: Long,
                  val period : Long )