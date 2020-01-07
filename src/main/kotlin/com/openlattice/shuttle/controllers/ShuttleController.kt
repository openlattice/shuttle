package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.client.serialization.SerializableSupplier
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.*
import com.openlattice.shuttle.control.Integration
import com.openlattice.shuttle.control.IntegrationStatus
import com.openlattice.shuttle.control.IntegrationUpdate
import org.springframework.web.bind.annotation.*
import java.util.*
import java.util.function.Supplier
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class ShuttleController : ShuttleApi, AuthorizingComponent {

    @Inject
    private lateinit var integrationService: IntegrationService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Timed
    @GetMapping(path = [INTEGRATION_NAME_PATH + TOKEN_PATH])
    override fun startIntegration(
            @PathVariable(INTEGRATION_NAME) integrationName: String,
            @PathVariable(TOKEN) token: String
    ): UUID {
        ensureAdminAccess()
        val tokenTest = jwtToken
        val normalizedName = normalizeIntegrationName(integrationName)
        return integrationService.loadCargo(normalizedName, tokenTest)
    }

    @Timed
    @GetMapping(path = [STATUS_PATH + JOB_ID_PATH])
    override fun pollIntegration(
            @PathVariable(JOB_ID) jobId: UUID
    ): IntegrationStatus {
        ensureAdminAccess()
        return integrationService.pollIntegrationStatus(jobId)
    }

    @Timed
    @GetMapping(path = [STATUS_PATH])
    override fun pollAllIntegrations(): Map<UUID, IntegrationStatus> {
        ensureAdminAccess()
        return integrationService.pollAllIntegrationStatuses()
    }

    @Timed
    @PostMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun createIntegrationDefinition(
            @PathVariable integrationName: String,
            @RequestBody integrationDefinition: Integration) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        integrationService.createIntegrationDefinition(normalizedName, integrationDefinition)
    }

    @Timed
    @GetMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun readIntegrationDefinition(
            @PathVariable integrationName: String ) : Integration {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        return integrationService.readIntegrationDefinition(normalizedName)
    }

    @Timed
    @PatchMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun updateIntegrationDefinition(
            @PathVariable integrationName: String,
            @RequestBody integrationUpdate: IntegrationUpdate) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        integrationService.updateIntegrationDefinition(normalizedName, integrationUpdate)
    }

    @Timed
    @DeleteMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun deleteIntegrationDefinition(
            @PathVariable integrationName: String
    ) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        integrationService.deleteIntegrationDefinition(normalizedName)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    private fun normalizeIntegrationName(integrationName: String): String {
        return integrationName.toLowerCase().trim()
    }

}
