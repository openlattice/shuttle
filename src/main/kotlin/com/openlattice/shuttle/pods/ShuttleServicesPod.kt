package com.openlattice.shuttle.pods

import com.dataloom.mappers.ObjectMappers
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.authorization.*
import com.openlattice.authorization.mapstores.ResolvedPrincipalTreesMapLoader
import com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.shuttle.MissionParameters
import com.openlattice.shuttle.RecurringIntegrationService
import com.openlattice.shuttle.mapstore.IntegrationsMapstore
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import javax.annotation.PostConstruct
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

    @Inject
    private lateinit var spml: SecurablePrincipalsMapLoader

    @Inject
    private lateinit var rptml: ResolvedPrincipalTreesMapLoader

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

    @Bean
    fun aclKeyReservationService() = HazelcastAclKeyReservationService(hazelcastInstance)

    @Bean
    fun principalService(): SecurePrincipalsManager = HazelcastPrincipalService(
            hazelcastInstance,
            aclKeyReservationService(),
            authorizationManager(),
            eventBus)

    @PostConstruct
    internal fun initPrincipals() {
        Principals.init(principalService(), hazelcastInstance)
    }

}