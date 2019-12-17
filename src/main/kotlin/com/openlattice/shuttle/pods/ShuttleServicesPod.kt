package com.openlattice.shuttle.pods

import com.dataloom.mappers.ObjectMappers
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.authorization.*
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.shuttle.MissionParameters
import com.openlattice.shuttle.IntegrationService
import com.openlattice.shuttle.logs.Blackbox
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
    private lateinit var missionParametersConfiguration: MissionParameters

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var executorService: ListeningExecutorService

    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Inject
    private lateinit var blackbox: Blackbox

    @Bean
    fun defaultObjectMapper() = ObjectMappers.getJsonMapper()

    @Bean
    fun authorizationQueryService() = AuthorizationQueryService(hds, hazelcastInstance)

    @Bean
    fun authorizationManager() = HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus)



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

    @Bean
    fun idGenerationService() = HazelcastIdGenerationService(hazelcastClientProvider, executorService)

    @Bean
    internal fun partitionManager() = PartitionManager(hazelcastInstance, hds)

    @Bean
    fun idService() = PostgresEntityKeyIdService(hazelcastClientProvider,
            executorService,
            hds,
            idGenerationService(),
            partitionManager())

    @Bean
    fun pgEdmManager() = PostgresEdmManager(hds, hazelcastInstance)

    @Bean
    fun entityTypeManager() = PostgresTypeManager(hds)

    @Bean
    fun schemaQueryService() = PostgresSchemaQueryService(hds)

    @Bean
    fun schemaManager() = HazelcastSchemaManager(hazelcastInstance, schemaQueryService())

    @Bean
    fun dataModelService() = EdmService(
                hds,
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                pgEdmManager(),
                entityTypeManager(),
                schemaManager()
        )

    @Bean
    fun entitySetManager() = EntitySetService(
            hazelcastInstance,
            eventBus,
            pgEdmManager(),
            aclKeyReservationService(),
            authorizationManager(),
            partitionManager(),
            dataModelService(),
            auditingConfiguration
    )

    @Bean
    fun integrationService() = IntegrationService(
            hazelcastInstance,
            missionParametersConfiguration,
            blackbox,
            hds,
            idService(),
            entitySetManager(),
            dataModelService())

    @PostConstruct
    internal fun initPrincipals() {
        Principals.init(principalService(), hazelcastInstance)
    }

}