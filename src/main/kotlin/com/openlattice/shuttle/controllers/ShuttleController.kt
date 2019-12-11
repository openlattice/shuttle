package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.*
import com.openlattice.shuttle.control.Integration
import org.springframework.web.bind.annotation.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class ShuttleController : ShuttleApi, AuthorizingComponent {

    @Inject
    private lateinit var recurringIntegrationService: RecurringIntegrationService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Timed
    @PatchMapping(path = [INTEGRATION_NAME_PATH])
    override fun startIntegration(
            @PathVariable(INTEGRATION_NAME) integrationName: String
    ) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        recurringIntegrationService.loadCargo(normalizedName)
    }

    @Timed
    @PostMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun createIntegrationDefinition(
            @PathVariable integrationName: String,
            @RequestBody integrationDefinition: Integration) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        recurringIntegrationService.createIntegrationDefinition(normalizedName, integrationDefinition)
    }

    @Timed
    @GetMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun readIntegrationDefinition(
            @PathVariable integrationName: String ) : Integration {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        return recurringIntegrationService.readIntegrationDefinition(normalizedName)
    }

    @Timed
    @PatchMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun updateIntegrationDefinition(integrationName: String, integrationDefinition: Integration) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        recurringIntegrationService.updateIntegrationDefinition(normalizedName, integrationDefinition)
    }

//    @Timed
//    @PatchMapping(path = [DEFINITION_PATH + FLIGHT_PATH + INTEGRATION_NAME_PATH + PATH_TO_FLIGHT_PATH])
//    override fun updateFlightWithinIntegrationDefinition(
//            integrationName: String,
//            pathToFlight: String
//    ) {
//        ensureAdminAccess()
//        val normalizedName = normalizeIntegrationName(integrationName)
//        recurringIntegrationService.updateFlightWithinIntegrationDefinition(normalizedName, pathToFlight)
//    }

    @Timed
    @PatchMapping(path = [DEFINITION_PATH + FLIGHT_PATH + INTEGRATION_NAME_PATH])
    override fun updateFlightWithinIntegrationDefinition(
            integrationName: String
    ) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        recurringIntegrationService.updateFlightWithinIntegrationDefinition(normalizedName)
    }


    @Timed
    @DeleteMapping(path = [DEFINITION_PATH + INTEGRATION_NAME_PATH])
    override fun deleteIntegrationDefinition(integrationName: String) {
        ensureAdminAccess()
        val normalizedName = normalizeIntegrationName(integrationName)
        recurringIntegrationService.deleteIntegrationDefinition(normalizedName)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

    private fun normalizeIntegrationName(integrationName: String): String {
        return integrationName.toLowerCase().trim()
    }

}
