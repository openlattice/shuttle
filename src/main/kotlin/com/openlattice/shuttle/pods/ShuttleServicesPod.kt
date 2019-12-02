package com.openlattice.shuttle.pods

import com.geekbeast.hazelcast.HazelcastClientProvider
import com.google.common.eventbus.EventBus
import com.google.common.util.concurrent.ListeningExecutorService
import com.hazelcast.core.HazelcastInstance
import com.kryptnostic.rhizome.pods.hazelcast.RegistryBasedHazelcastInstanceConfigurationPod
import com.openlattice.ResourceConfigurationLoader
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.auditing.pods.AuditingConfigurationPod
import com.openlattice.authorization.AuthorizationQueryService
import com.openlattice.authorization.HazelcastAclKeyReservationService
import com.openlattice.authorization.HazelcastAuthorizationService
import com.openlattice.data.DataGraphService
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.PostgresEntityDataQueryService
import com.openlattice.data.storage.PostgresEntityDatastore
import com.openlattice.data.storage.PostgresEntitySetSizesTask
import com.openlattice.data.storage.partitions.PartitionManager
import com.openlattice.datastore.pods.ByteBlobServicePod
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.PostgresEdmManager
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.edm.schemas.postgres.PostgresSchemaQueryService
import com.openlattice.graph.Graph
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.jdbc.JdbcPod
import com.openlattice.shuttle.FlightConfiguration
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
@Import(value = [ByteBlobServicePod::class, AuditingConfigurationPod::class])
class ShuttleServicesPod {

    @Inject
    private lateinit var hds: HikariDataSource

    @Inject
    private lateinit var hazelcastInstance: HazelcastInstance

    @Inject
    private lateinit var hazelcastClientProvider: HazelcastClientProvider

    @Inject
    private lateinit var executor: ListeningExecutorService

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @Inject
    private lateinit var eventBus: EventBus

    @Inject
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Bean
    fun aclKeyReservationService() = HazelcastAclKeyReservationService(hazelcastInstance)

    @Bean
    fun pgEdmManager(): PostgresEdmManager {
        val pgEdmManager = PostgresEdmManager(hds, hazelcastInstance)
        eventBus.register(pgEdmManager)
        return pgEdmManager
    }

    @Bean
    fun authorizationQueryService() = AuthorizationQueryService(hds, hazelcastInstance)

    @Bean
    fun authorizationManager() = HazelcastAuthorizationService(hazelcastInstance, authorizationQueryService(), eventBus)

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
            schemaManager())

    @Bean
    fun entitySetManager() = EntitySetService(
            hazelcastInstance,
            eventBus,
            pgEdmManager(),
            aclKeyReservationService(),
            authorizationManager(),
            partitionManager(),
            dataModelService(),
            auditingConfiguration)

    @Bean
    fun partitionManager() = PartitionManager(hazelcastInstance, hds)

    @Bean
    fun dataQueryService() = PostgresEntityDataQueryService(hds, byteBlobDataManager, partitionManager())

    @Bean
    fun postgresEntitySetSizeCacheManager() = PostgresEntitySetSizesTask()

    @Bean
    fun entityDatastore() = PostgresEntityDatastore(dataQueryService(), pgEdmManager(), entitySetManager())

    @Bean
    fun idGenerationService() = HazelcastIdGenerationService(hazelcastClientProvider, executor)

    @Bean
    fun idService() = PostgresEntityKeyIdService(hazelcastClientProvider, executor, hds, idGenerationService(), partitionManager())

    @Bean
    fun graphApi() = Graph(hds, entitySetManager(), partitionManager())

    @Bean
    fun dataGraphService() = DataGraphService(graphApi(), idService(), entityDatastore(), postgresEntitySetSizeCacheManager())

    @Bean
    fun flightConfiguration(): FlightConfiguration = ResourceConfigurationLoader.loadConfiguration(FlightConfiguration::class.java)

    @Bean
    fun recurringIntegrationService() = RecurringIntegrationService(hds, entitySetManager(), dataGraphService(), flightConfiguration())

}