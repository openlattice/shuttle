package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.*
import com.openlattice.shuttle.control.Integration
import org.springframework.web.bind.annotation.*
import java.util.*
import javax.inject.Inject

@RestController
@RequestMapping(CONTROLLER)
class ShuttleController : ShuttleApi, AuthorizingComponent {

    @Inject
    private lateinit var recurringIntegrationService: RecurringIntegrationService

    @Inject
    private lateinit var authorizationManager: AuthorizationManager

    @Timed
    @PatchMapping(path = [FLIGHT_NAME_PATH])
    override fun startIntegration(
            @PathVariable(FLIGHT_NAME) flightName: String
    ) {
        ensureAdminAccess()
        recurringIntegrationService.loadCargo(flightName.toLowerCase())
    }

    @Timed
    @PostMapping(path = [DEFINITION + FLIGHT_NAME_PATH])
    override fun createIntegrationDefinition(
            @PathVariable flightName: String,
            @RequestBody integrationDefinition: Integration) {
        ensureAdminAccess()
        recurringIntegrationService.createIntegration(flightName.toLowerCase(), integrationDefinition)
    }

    @Timed
    @PatchMapping(path = [DEFINITION + FLIGHT_NAME_PATH])
    override fun updateIntegrationDefinition(flightName: String, integrationDefinition: Integration) {
        ensureAdminAccess()
        recurringIntegrationService.updateIntegration(flightName.toLowerCase(), integrationDefinition)
    }

    @Timed
    @DeleteMapping(path = [DEFINITION + FLIGHT_NAME_PATH])
    override fun deleteIntegrationDefinition(flightName: String) {
        ensureAdminAccess()
        recurringIntegrationService.deleteIntegration(flightName.toLowerCase())
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}
