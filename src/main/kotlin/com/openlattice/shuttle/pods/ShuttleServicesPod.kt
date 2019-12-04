package com.openlattice.shuttle.pods

import com.hazelcast.core.HazelcastInstance
import com.openlattice.shuttle.RecurringIntegrationService
import com.openlattice.shuttle.mapstore.IntegrationsMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
class ShuttleServicesPod {

    @Inject
    private lateinit var hds: HikariDataSource

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Bean
    fun recurringIntegrationService() = RecurringIntegrationService(hazelcastInstance)

    @Bean
    fun integrationsMapstore() = IntegrationsMapstore(hds)

}