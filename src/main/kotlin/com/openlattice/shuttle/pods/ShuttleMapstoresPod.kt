package com.openlattice.shuttle.pods

import com.kryptnostic.rhizome.mapstores.SelfRegisteringMapStore
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationJobsMapstore
import com.openlattice.hazelcast.mapstores.shuttle.IntegrationsMapstore
import com.openlattice.postgres.PostgresPod
import com.openlattice.shuttle.Integration
import com.openlattice.shuttle.IntegrationJob
import com.zaxxer.hikari.HikariDataSource
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import java.util.*
import javax.inject.Inject

@Configuration
@Import(PostgresPod::class)
class ShuttleMapstoresPod {
    @Inject
    private lateinit var hds: HikariDataSource

    @Bean
    fun integrationsMapstore() : SelfRegisteringMapStore<String, Integration> = IntegrationsMapstore(hds)

    @Bean
    fun integrationJobsMapstore() : SelfRegisteringMapStore<UUID, IntegrationJob> = IntegrationJobsMapstore(hds)
}