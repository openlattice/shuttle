package com.openlattice.shuttle.pods

import com.auth0.client.mgmt.ManagementAPI
import com.codahale.metrics.MetricRegistry
import com.geekbeast.auth0.Auth0Pod
import com.geekbeast.auth0.Auth0TokenProvider
import com.geekbeast.auth0.ManagementApiProvider
import com.geekbeast.auth0.RefreshingAuth0TokenProvider
import com.geekbeast.authentication.Auth0Configuration
import com.geekbeast.hazelcast.HazelcastClientProvider
import com.geekbeast.jdbc.DataSourceManager
import com.geekbeast.mappers.mappers.ObjectMappers
import com.geekbeast.rhizome.pods.ConfigurationLoader
import com.geekbeast.tasks.PostConstructInitializerTaskDependencies
import com.google.common.eventbus.EventBus
import com.hazelcast.core.HazelcastInstance
import com.openlattice.assembler.Assembler
import com.openlattice.assembler.AssemblerConfiguration
import com.openlattice.assembler.UserRoleSyncTaskDependencies
import com.openlattice.assembler.pods.AssemblerConfigurationPod
import com.openlattice.assembler.tasks.UsersAndRolesInitializationTask
import com.openlattice.auditing.AuditingConfiguration
import com.openlattice.authorization.*
import com.openlattice.authorization.initializers.AuthorizationInitializationDependencies
import com.openlattice.authorization.initializers.AuthorizationInitializationTask
import com.openlattice.authorization.mapstores.ResolvedPrincipalTreesMapLoader
import com.openlattice.authorization.mapstores.SecurablePrincipalsMapLoader
import com.openlattice.collaborations.CollaborationDatabaseManager
import com.openlattice.collaborations.CollaborationService
import com.openlattice.collaborations.PostgresCollaborationDatabaseService
import com.openlattice.conductor.rpc.ConductorConfiguration
import com.openlattice.conductor.rpc.ConductorElasticsearchApi
import com.openlattice.data.ids.PostgresEntityKeyIdService
import com.openlattice.data.storage.ByteBlobDataManager
import com.openlattice.data.storage.DataSourceResolver
import com.openlattice.data.storage.aws.AwsDataSinkService
import com.openlattice.datasets.DataSetService
import com.openlattice.datastore.services.EdmService
import com.openlattice.datastore.services.EntitySetService
import com.openlattice.edm.properties.PostgresTypeManager
import com.openlattice.edm.schemas.manager.HazelcastSchemaManager
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationJobsMapstore
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationsMapstore
import com.openlattice.ids.HazelcastIdGenerationService
import com.openlattice.ids.HazelcastLongIdService
import com.openlattice.notifications.sms.PhoneNumberService
import com.openlattice.organizations.HazelcastOrganizationService
import com.openlattice.organizations.roles.HazelcastPrincipalService
import com.openlattice.organizations.roles.SecurePrincipalsManager
import com.openlattice.organizations.tasks.OrganizationsInitializationTask
import com.openlattice.postgres.external.*
import com.openlattice.scrunchie.search.ConductorElasticsearchImpl
import com.openlattice.shuttle.IntegrationService
import com.openlattice.shuttle.MissionParameters
import com.openlattice.shuttle.logs.Blackbox
import com.openlattice.users.Auth0UserListingService
import com.openlattice.users.LocalUserListingService
import com.openlattice.users.UserListingService
import com.openlattice.users.export.Auth0ApiExtension
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import javax.annotation.PostConstruct
import javax.inject.Inject

/**
 *
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
@Configuration
@Import(
    AssemblerConfigurationPod::class,
    Auth0Pod::class
)
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
    private lateinit var auditingConfiguration: AuditingConfiguration

    @Inject
    private lateinit var blackbox: Blackbox

    @Inject
    private lateinit var spml: SecurablePrincipalsMapLoader

    @Inject
    private lateinit var rptml: ResolvedPrincipalTreesMapLoader

    @Inject
    private lateinit var metricRegistry: MetricRegistry

    @Inject
    private lateinit var auth0Configuration: Auth0Configuration

    @Inject
    private lateinit var configurationLoader: ConfigurationLoader

    @Inject
    private lateinit var assemblerConfiguration: AssemblerConfiguration

    @Inject
    private lateinit var byteBlobDataManager: ByteBlobDataManager

    @Inject
    private lateinit var externalDbConnMan: ExternalDatabaseConnectionManager

    @Inject
    private lateinit var dataSourceManager: DataSourceManager

    @Bean
    fun conductorConfiguration(): ConductorConfiguration {
        return configurationLoader.logAndLoad("conductor", ConductorConfiguration::class.java)
    }

    @Bean
    fun defaultObjectMapper() = ObjectMappers.getJsonMapper()

    @Bean
    fun authorizationManager() = HazelcastAuthorizationService(hazelcastInstance, eventBus, principalsMapManager())

    @Bean
    fun phoneNumberService(): PhoneNumberService {
        return PhoneNumberService(hazelcastInstance)
    }

    @Bean
    fun longIdService(): HazelcastLongIdService {
        return HazelcastLongIdService(hazelcastClientProvider)
    }

    @Bean
    fun dbcs(): DbCredentialService {
        return DbCredentialService(hazelcastInstance, longIdService())
    }

    @Bean
    fun assembler(): Assembler {
        return Assembler(
                dbcs(),
                authorizationManager(),
                principalService(),
                dbQueryManager(),
                metricRegistry,
                hazelcastInstance,
                eventBus
        )
    }

    @Bean
    fun dbQueryManager(): DatabaseQueryManager {
        return PostgresDatabaseQueryService(
                assemblerConfiguration,
                externalDbConnMan,
                principalService(),
                dbcs()
        )
    }

    @Bean
    fun collaborationDatabaseManager(): CollaborationDatabaseManager {
        return PostgresCollaborationDatabaseService(
                hazelcastInstance,
                dbQueryManager(),
                externalDbConnMan,
                authorizationManager(),
                externalDatabasePermissionsManager(),
                principalService(),
                dbcs(),
                assemblerConfiguration
        )
    }

    @Bean
    fun collaborationService(): CollaborationService {
        return CollaborationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                collaborationDatabaseManager()
        )
    }

    @Bean
    fun organizationsManager(): HazelcastOrganizationService {
        return HazelcastOrganizationService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalService(),
                phoneNumberService(),
                assembler(),
                collaborationService()
        )
    }

    @Bean
    fun auth0TokenProvider(): Auth0TokenProvider {
        return RefreshingAuth0TokenProvider(auth0Configuration)
    }

    @Bean
    fun managementApiProvider(): ManagementApiProvider {
        return ManagementApiProvider(auth0TokenProvider(), auth0Configuration)
    }

    @Bean
    fun userListingService(): UserListingService {
        val config = auth0Configuration
        return if (config.managementApiUrl.contains(Auth0Configuration.NO_SYNC_URL)) {
            LocalUserListingService(config)
        } else {
            val auth0Token = auth0TokenProvider().token
            Auth0UserListingService(
                    managementApiProvider(),
                    Auth0ApiExtension(config.domain, auth0Token)
            )
        }
    }

    @Bean
    fun postInitializerDependencies(): PostConstructInitializerTaskDependencies {
        return PostConstructInitializerTaskDependencies()
    }

    @Bean
    fun postInitializerTask(): PostConstructInitializerTaskDependencies.PostConstructInitializerTask {
        return PostConstructInitializerTaskDependencies.PostConstructInitializerTask()
    }

    @Bean
    fun assemblerDependencies(): UserRoleSyncTaskDependencies {
        return UserRoleSyncTaskDependencies(
                dbcs(),
                externalDbConnMan,
                externalDatabasePermissionsManager(),
                principalService()
        );
    }

    @Bean
    fun assemblerInitializationTask(): UsersAndRolesInitializationTask {
        return UsersAndRolesInitializationTask()
    }

    @Bean
    fun organizationBootstrap(): OrganizationsInitializationTask {
        return OrganizationsInitializationTask()
    }

    @Bean
    fun authorizationBootstrapDependencies(): AuthorizationInitializationDependencies {
        return AuthorizationInitializationDependencies(principalService())
    }

    @Bean
    fun authorizationBootstrap(): AuthorizationInitializationTask {
        return AuthorizationInitializationTask()
    }

    @Bean
    fun integrationsMapstore() = IntegrationsMapstore(hds)

    @Bean
    fun integrationJobsMapstore() = IntegrationJobsMapstore(hds)

    @Bean
    fun aclKeyReservationService() = HazelcastAclKeyReservationService(hazelcastInstance)

    @Bean
    fun principalsMapManager(): PrincipalsMapManager {
        return HazelcastPrincipalsMapManager(hazelcastInstance, aclKeyReservationService())
    }

    @Bean
    fun principalService(): SecurePrincipalsManager {
        return HazelcastPrincipalService(
                hazelcastInstance,
                aclKeyReservationService(),
                authorizationManager(),
                principalsMapManager(),
                externalDatabasePermissionsManager()
        )
    }

    @Bean
    fun externalDatabasePermissionsManager(): ExternalDatabasePermissioningService {
        return ExternalDatabasePermissioner(
                hazelcastInstance,
                externalDbConnMan,
                dbcs(),
                principalsMapManager()
        )
    }

    @Bean
    fun idGenerationService() = HazelcastIdGenerationService(hazelcastClientProvider)

    @Bean
    fun idService() = PostgresEntityKeyIdService(
            dataSourceResolver(),
            idGenerationService()
    )

    @Bean
    fun entityTypeManager() = PostgresTypeManager(hds, hazelcastInstance)

    @Bean
    fun schemaQueryService() = entityTypeManager()

    @Bean
    fun schemaManager() = HazelcastSchemaManager(hazelcastInstance, schemaQueryService())

    @Bean
    fun datasetService() = DataSetService(hazelcastInstance, elasticsearchApi())

    @Bean
    fun dataModelService() = EdmService(
            hazelcastInstance,
            aclKeyReservationService(),
            authorizationManager(),
            entityTypeManager(),
            schemaManager(),
            datasetService()
    )

    @Bean
    fun dataSourceResolver(): DataSourceResolver {
        return DataSourceResolver(hazelcastInstance, dataSourceManager)
    }

    @Bean
    fun entitySetManager() = EntitySetService(
            hazelcastInstance,
            eventBus,
            aclKeyReservationService(),
            authorizationManager(),
            dataModelService(),
            hds,
            datasetService(),
            auditingConfiguration
    )

    @Bean
    internal fun awsDataSinkService(): AwsDataSinkService {
        return AwsDataSinkService(
                byteBlobDataManager,
                dataSourceResolver()
        )
    }

    @Bean
    fun integrationService(): IntegrationService {
        return IntegrationService(
                hazelcastInstance,
                missionParametersConfiguration,
                idService(),
                entitySetManager(),
                aclKeyReservationService(),
                awsDataSinkService(),
                blackbox
        )
    }

    @Bean
    fun elasticsearchApi(): ConductorElasticsearchApi {
        return ConductorElasticsearchImpl(conductorConfiguration().searchConfiguration)
    }

    @PostConstruct
    internal fun initPrincipals() {
        Principals.init(principalService(), hazelcastInstance)
        IntegrationService.init(blackbox, dataModelService())
    }

}
