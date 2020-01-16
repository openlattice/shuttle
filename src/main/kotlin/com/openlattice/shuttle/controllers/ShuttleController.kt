package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.*
import com.openlattice.shuttle.Integration
import com.openlattice.shuttle.IntegrationJob
import com.openlattice.shuttle.IntegrationStatus
import com.openlattice.shuttle.IntegrationUpdate
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class ShuttleController : ShuttleApi, AuthorizingComponent {

    @Inject
    private lateinit var integrationService: IntegrationService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Timed
    @GetMapping(path = [INTEGRATION_NAME_PATH + INTEGRATION_KEY_PATH + CALLBACK_PATH])
    override fun enqueueIntegration(
            @PathVariable(INTEGRATION_NAME) integrationName: String,
            @PathVariable(INTEGRATION_KEY) integrationKey: UUID,
            @PathVariable(CALLBACK) callbackUrl: Optional<String>
    ): UUID {
        val normalizedName = normalizeIntegrationName(integrationName)
        return integrationService.enqueueIntegrationJob(normalizedName, integrationKey)
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
    override fun pollAllIntegrations(): Map<UUID, IntegrationJob> {
        ensureAdminAccess()
        return integrationService.pollAllIntegrationStatuses()
    }

    @Timed
    @DeleteMapping(path = [STATUS_PATH + JOB_ID_PATH])
    override fun deleteIntegrationJobStatus(
            @PathVariable jobId: UUID
    ) {
       ensureAdminAccess()
        return integrationService.deleteIntegrationJobStatus(jobId)
    }

    @Timed
    @PostMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun createIntegrationDefinition(
            @PathVariable integrationName: String,
            @RequestBody integrationDefinition: Integration): UUID {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        return integrationService.createIntegrationDefinition(normalizedName, integrationDefinition)
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
        return integrationName.trim().toLowerCase()
    }

}
