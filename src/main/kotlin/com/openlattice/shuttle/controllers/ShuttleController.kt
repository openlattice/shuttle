package com.openlattice.shuttle.controllers

import com.codahale.metrics.annotation.Timed
import com.openlattice.authorization.AuthorizationManager
import com.openlattice.authorization.AuthorizingComponent
import com.openlattice.shuttle.*
import com.openlattice.shuttle.api.*
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
            @PathVariable(FLIGHT_NAME) flightName: String,
            @RequestBody lastRow: String
    ) {
        ensureAdminAccess()
        recurringIntegrationService.loadCargo(flightName, lastRow)
    }

    override fun getAuthorizationManager(): AuthorizationManager {
        return authorizationManager
    }

}
