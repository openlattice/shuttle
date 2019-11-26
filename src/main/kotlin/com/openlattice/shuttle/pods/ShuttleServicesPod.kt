package com.openlattice.shuttle.pods

import com.openlattice.jdbc.JdbcPod
import com.openlattice.shuttle.RecurringIntegrationService
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
@Import( value = [JdbcPod::class] )
class ShuttleServicesPod {

    @Inject
    private lateinit var hds: HikariDataSource

    @Bean
    fun recurringIntegrationService() = RecurringIntegrationService(hds)

}