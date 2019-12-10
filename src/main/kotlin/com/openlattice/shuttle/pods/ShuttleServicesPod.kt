package com.openlattice.shuttle.pods

import com.dataloom.mappers.ObjectMappers
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizationQueryService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.shuttle.MissionParameters
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

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var  missionParametersConfiguration: MissionParameters

    @Bean
    fun defaultObjectMapper() = ObjectMappers.getJsonMapper()

    @Bean
    fun authorizationQueryService() = AuthorizationQueryService(hds, hazelcastInstance)

    @Bean
    fun authorizationManager() = HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus)

    @Bean
    fun recurringIntegrationService() = RecurringIntegrationService(hazelcastInstance, missionParametersConfiguration)

    @Bean
    fun integrationsMapstore() = IntegrationsMapstore(hds)

}